"""ExecutionAgent — Order placement, position lifecycle, fill/close handling.

This agent receives approved orders from RiskAgent, places bracket orders
via IBKRClientAgent, and tracks positions through their full lifecycle.

It listens for:
  - order_filled (from IBKRClientAgent) — records entry in SQLite, sends Telegram alert
  - position_closed (from IBKRClientAgent) — calculates P&L, updates SQLite, sends alert
"""

import asyncio
from datetime import datetime

from loguru import logger

from agents.base_agent import BaseAgent, Message
from db import database as db
from utils.helpers import format_currency, format_pct, pnl_emoji


class ExecutionAgent(BaseAgent):
    def __init__(self, config: dict, orchestrator=None):
        super().__init__("ExecutionAgent", orchestrator)
        self.max_hold_minutes = config.get("max_hold_minutes", 240)
        # symbol -> trade dict
        self._open_trades: dict[str, dict] = {}
        # order_id -> symbol mapping for fill correlation
        self._order_symbol_map: dict[int, str] = {}

    async def run(self):
        """Main loop: process inbox and monitor positions."""
        asyncio.create_task(self._monitor_positions())

        while self._running:
            await self._process_inbox()
            await asyncio.sleep(0.1)

    async def _on_order_filled(self, payload: dict):
        """
        Handle an order fill event from IBKRClientAgent.
        This is triggered when the PARENT (entry) order of a bracket fills.
        """
        symbol = payload["symbol"]
        fill_price = payload["fill_price"]
        quantity = payload["quantity"]
        side = payload["side"]  # "BOT" or "SLD"
        order_id = payload.get("order_id")
        exec_time = payload.get("exec_time", datetime.utcnow().isoformat())

        # Determine direction from fill side
        # BOT = bought = long entry; SLD = sold = short entry
        direction = "LONG" if side == "BOT" else "SHORT"
        action = "BUY" if side == "BOT" else "SELL"

        # Look up the original order details from pending trades
        pending = self._open_trades.get(symbol, {})
        stop_loss = pending.get("stop_loss", 0)
        take_profit = pending.get("take_profit", 0)
        risk_dollars = pending.get("risk_dollars", 0)
        reward_dollars = pending.get("reward_dollars", 0)
        rr_ratio = pending.get("rr_ratio", 0)
        confidence = pending.get("confidence", 0)
        reasoning = pending.get("reasoning", "")
        indicators_bullish = pending.get("indicators_bullish", [])
        indicators_bearish = pending.get("indicators_bearish", [])
        nav = pending.get("nav", 10000)

        # Update the trade record with actual fill price
        trade_data = {
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "entry_price": fill_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "risk_dollars": risk_dollars,
            "reward_dollars": reward_dollars,
            "confidence": confidence,
            "reasoning": reasoning,
            "indicators_bullish": indicators_bullish,
            "indicators_bearish": indicators_bearish,
            "entry_time": exec_time,
        }

        # Insert trade to SQLite
        trade_id = await db.insert_trade(trade_data)

        # Update open trades with actual fill data and db ID
        self._open_trades[symbol] = {
            **pending,
            "db_id": trade_id,
            "direction": direction,
            "entry_price": fill_price,
            "quantity": quantity,
            "entry_time": exec_time,
            "order_id": order_id,
            "filled": True,
        }

        # Calculate display values
        sl_pct = abs(stop_loss - fill_price) / fill_price * 100 if fill_price else 0
        tp_pct = abs(take_profit - fill_price) / fill_price * 100 if fill_price else 0
        risk_nav_pct = (risk_dollars / nav * 100) if nav else 0

        # Send Telegram alert
        alert_text = (
            f"\U0001f7e2 <b>NEW POSITION</b>\n"
            f"<b>{symbol}</b> {direction} x{quantity} @ ${fill_price:.2f}\n"
            f"Stop:   ${stop_loss:.2f} ({sl_pct:.1f}%)\n"
            f"Target: ${take_profit:.2f} ({tp_pct:.1f}%)\n"
            f"R:R 1:{rr_ratio:.1f} | Risk: ${risk_dollars:.2f} ({risk_nav_pct:.1f}%)"
        )
        self.send("TelegramAgent", "send_message", {"text": alert_text})

        # Notify RiskAgent of new position
        self.send("RiskAgent", "position_opened", {
            "symbol": symbol,
            "direction": direction,
            "quantity": quantity,
            "entry_price": fill_price,
        })

        logger.info(f"Position opened: {direction} {symbol} x{quantity} @ ${fill_price:.2f}")

    async def _on_position_closed(self, payload: dict):
        """
        Handle a position close event from IBKRClientAgent.
        This is triggered when a bracket child order (SL or TP) fills,
        or when a manual/EOD close fills.
        """
        symbol = payload["symbol"]
        fill_price = payload["fill_price"]
        exit_reason = payload.get("exit_reason", "MANUAL")
        exec_time = payload.get("exec_time", datetime.utcnow().isoformat())

        trade = self._open_trades.get(symbol)
        if not trade:
            logger.warning(f"Position close for {symbol} but no open trade found")
            return

        entry_price = trade.get("entry_price", 0)
        quantity = trade.get("quantity", 0)
        direction = trade.get("direction", "LONG")
        db_id = trade.get("db_id")
        entry_time = trade.get("entry_time", "")

        # Calculate P&L
        if direction == "LONG":
            pnl = (fill_price - entry_price) * quantity
        else:  # SHORT
            pnl = (entry_price - fill_price) * quantity

        pnl_pct = (pnl / (entry_price * quantity) * 100) if (entry_price * quantity) else 0

        # Calculate hold time
        hold_minutes = 0
        try:
            entry_dt = datetime.fromisoformat(str(entry_time))
            exit_dt = datetime.fromisoformat(str(exec_time))
            hold_minutes = (exit_dt - entry_dt).total_seconds() / 60
        except Exception:
            pass

        # Calculate achieved R:R
        stop_loss = trade.get("stop_loss", 0)
        sl_dist = abs(entry_price - stop_loss) if stop_loss else 1
        rr_achieved = abs(pnl / quantity) / sl_dist if sl_dist and quantity else 0

        # Update SQLite
        if db_id:
            await db.update_trade_exit(db_id, {
                "exit_price": fill_price,
                "pnl": round(pnl, 2),
                "rr_achieved": round(rr_achieved, 2),
                "hold_minutes": round(hold_minutes, 1),
                "exit_reason": exit_reason,
                "exit_time": exec_time,
            })

        # Determine exit reason emoji
        reason_display = {
            "TP": "Hit target \u2705",
            "SL": "Stopped out \U0001f6d1",
            "MANUAL": "Closed manually \u270b",
            "EOD": "EOD \U0001f554",
            "SHUTDOWN": "Shutdown \u26a0\ufe0f",
        }.get(exit_reason, exit_reason)

        emoji = pnl_emoji(pnl)

        # Send Telegram alert
        alert_text = (
            f"{emoji} <b>POSITION CLOSED</b>\n"
            f"<b>{symbol}</b> {direction} x{quantity}\n"
            f"Opened: ${entry_price:.2f} \u2192 Closed: ${fill_price:.2f}\n"
            f"P&L: {format_currency(pnl)} ({format_pct(pnl_pct)})\n"
            f"{reason_display}"
        )
        self.send("TelegramAgent", "send_message", {"text": alert_text})

        # Notify RiskAgent
        self.send("RiskAgent", "position_closed_notify", {"symbol": symbol})

        # Remove from open trades
        del self._open_trades[symbol]

        logger.info(
            f"Position closed: {direction} {symbol} x{quantity} @ ${fill_price:.2f} "
            f"| P&L: {format_currency(pnl)} | Reason: {exit_reason}"
        )

    async def _monitor_positions(self):
        """Check for positions exceeding max hold time and auto-close."""
        while self._running:
            await asyncio.sleep(30)
            now = datetime.utcnow()

            for symbol, trade in list(self._open_trades.items()):
                if not trade.get("filled"):
                    continue

                entry_time = trade.get("entry_time")
                if not entry_time:
                    continue

                try:
                    entry_dt = datetime.fromisoformat(str(entry_time))
                    held_minutes = (now - entry_dt).total_seconds() / 60

                    if held_minutes >= self.max_hold_minutes:
                        logger.info(f"Auto-closing {symbol} — held {held_minutes:.0f}m (max {self.max_hold_minutes}m)")
                        await self.close_position(symbol, "EOD")
                except Exception as e:
                    logger.warning(f"Error checking hold time for {symbol}: {e}")

    async def close_position(self, symbol: str, reason: str = "MANUAL"):
        """Request IBKRClientAgent to close a position."""
        self.send("IBKRClientAgent", "close_position", {
            "symbol": symbol,
            "reason": reason,
        })

    async def close_all_positions(self, reason: str = "SHUTDOWN"):
        """Close all open positions."""
        for symbol in list(self._open_trades.keys()):
            await self.close_position(symbol, reason)

    def get_open_positions_summary(self, market_data: dict = None) -> list[dict]:
        """Get summary of all open positions for dashboard display."""
        positions = []
        for symbol, trade in self._open_trades.items():
            if not trade.get("filled"):
                continue

            entry_price = trade.get("entry_price", 0)
            quantity = trade.get("quantity", 0)
            direction = trade.get("direction", "LONG")
            current_price = entry_price  # Default to entry

            if market_data and symbol in market_data:
                current_price = market_data[symbol].get("last", entry_price) or entry_price

            if direction == "LONG":
                pnl = (current_price - entry_price) * quantity
            else:
                pnl = (entry_price - current_price) * quantity

            pnl_pct = (pnl / (entry_price * quantity) * 100) if (entry_price * quantity) else 0

            positions.append({
                "symbol": symbol,
                "direction": direction,
                "quantity": quantity,
                "entry_price": entry_price,
                "current_price": current_price,
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
                "stop_loss": trade.get("stop_loss", 0),
                "take_profit": trade.get("take_profit", 0),
            })

        return positions

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "approved_order":
            # Store pending trade details before placing order
            symbol = payload["symbol"]
            self._open_trades[symbol] = {
                **payload,
                "filled": False,
                "pending_time": datetime.utcnow().isoformat(),
            }

            # Map order for fill correlation
            self.send("IBKRClientAgent", "place_bracket_order", {
                "symbol": symbol,
                "action": payload["action"],
                "quantity": payload["quantity"],
                "entry_price": payload["entry_price"],
                "stop_loss": payload["stop_loss"],
                "take_profit": payload["take_profit"],
            })
            logger.info(f"Bracket order sent for {symbol}: {payload['action']} x{payload['quantity']}")

        elif msg_type == "order_filled":
            # ── Known Gap #1 RESOLVED: Handle fill events from IBKRClientAgent ──
            fill_type = payload.get("fill_type", "entry")
            if fill_type == "entry":
                await self._on_order_filled(payload)
            elif fill_type == "close":
                # Standalone close (manual/market close)
                await self._on_position_closed(payload)

        elif msg_type == "position_closed":
            # ── Known Gap #2 RESOLVED: Handle position close from bracket child fills ──
            await self._on_position_closed(payload)

        elif msg_type == "close_position":
            symbol = payload.get("symbol")
            reason = payload.get("reason", "MANUAL")
            await self.close_position(symbol, reason)

        elif msg_type == "close_all_positions":
            reason = payload.get("reason", "SHUTDOWN")
            await self.close_all_positions(reason)

        elif msg_type == "market_close":
            # Auto-close all positions at market close
            await self.close_all_positions("EOD")

        elif msg_type == "get_positions_summary":
            market_data = payload.get("market_data", {})
            summary = self.get_open_positions_summary(market_data)
            self.send(message.sender, "positions_summary_response", {
                "positions": summary,
            })

        elif msg_type == "bracket_order_response":
            result = payload.get("result")
            original = payload.get("original_order", {})
            symbol = original.get("symbol")
            if result and symbol in self._open_trades:
                self._open_trades[symbol]["parent_order_id"] = result.get("parent_order_id")
                self._open_trades[symbol]["sl_order_id"] = result.get("sl_order_id")
                self._open_trades[symbol]["tp_order_id"] = result.get("tp_order_id")
                logger.info(f"Bracket order IDs stored for {symbol}")
            elif not result:
                logger.warning(f"Bracket order failed for {symbol}")
                self._open_trades.pop(symbol, None)
