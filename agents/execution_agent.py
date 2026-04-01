"""ExecutionAgent — Order placement, position lifecycle, fill/close handling.

This agent receives approved orders from RiskAgent, places bracket orders
via IBKRClientAgent, and tracks positions through their full lifecycle.

It listens for:
  - order_filled (from IBKRClientAgent) — records entry in SQLite, sends Telegram alert
  - position_closed (from IBKRClientAgent) — calculates P&L, updates SQLite, sends alert
  - close_position_response (from IBKRClientAgent) — handles failed close attempts
  - ibkr_reconnected (broadcast) — restores positions from IBKR on reconnect
"""

import asyncio
from datetime import datetime, timezone

from loguru import logger

from agents.base_agent import BaseAgent, Message
from db import database as db
from utils.helpers import format_currency, format_pct, pnl_emoji


def _parse_naive_dt(dt_str: str) -> datetime:
    """Strip timezone info and return a naive datetime for arithmetic."""
    s = str(dt_str).strip()
    for suffix in ("+00:00", "Z", "+0000"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    # IB sometimes returns "YYYYMMDD HH:MM:SS"
    if len(s) == 17 and s[8] == " ":
        return datetime.strptime(s, "%Y%m%d %H:%M:%S")
    return datetime.fromisoformat(s)


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

        logger.info(
            f"ENTRY FILL: {direction} {symbol} x{quantity} @ ${fill_price:.2f} "
            f"| SL=${stop_loss:.2f} TP=${take_profit:.2f} | db_id={trade_id}"
        )
        logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")

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
            logger.warning(f"Position close for {symbol} but no open trade found — ignoring")
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

        # Calculate hold time using timezone-safe parser
        hold_minutes = 0
        try:
            entry_dt = _parse_naive_dt(entry_time)
            exit_dt = _parse_naive_dt(exec_time)
            hold_minutes = (exit_dt - entry_dt).total_seconds() / 60
        except Exception as e:
            logger.warning(f"Could not parse hold time for {symbol}: {e}")

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
            f"EXIT FILL: {direction} {symbol} x{quantity} @ ${fill_price:.2f} "
            f"| P&L: {format_currency(pnl)} | Reason: {exit_reason} | held {hold_minutes:.0f}m"
        )
        logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")

    async def _monitor_positions(self):
        """Check for positions exceeding max hold time and auto-close."""
        while self._running:
            await asyncio.sleep(30)
            now = datetime.utcnow()

            for symbol, trade in list(self._open_trades.items()):
                if not trade.get("filled"):
                    continue

                # Skip if already waiting for a close fill
                if trade.get("_closing"):
                    logger.debug(f"Skipping {symbol} — close already in flight")
                    continue

                entry_time = trade.get("entry_time")
                if not entry_time:
                    continue

                try:
                    entry_dt = _parse_naive_dt(entry_time)
                    held_minutes = (now - entry_dt).total_seconds() / 60

                    if held_minutes >= self.max_hold_minutes:
                        logger.info(
                            f"Auto-closing {symbol} — held {held_minutes:.0f}m "
                            f"(max {self.max_hold_minutes}m)"
                        )
                        trade["_closing"] = True
                        await self.close_position(symbol, "EOD")
                except Exception as e:
                    logger.warning(f"Error checking hold time for {symbol}: {e}")

    async def _restore_positions(self):
        """
        On ibkr_reconnected: request account snapshot from IBKRClientAgent
        and reconcile IBKR live positions with _open_trades.
        A separate account_response handler does the actual reconciliation.
        """
        logger.info("Requesting account snapshot to restore positions after reconnect")
        self.send("IBKRClientAgent", "get_account", {})

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
            symbol = payload["symbol"]

            # Guard: don't open a duplicate position
            if symbol in self._open_trades:
                logger.warning(f"Duplicate order ignored: {symbol} already in _open_trades")
                return

            # Guard: only place orders during market hours (NYSE, UTC-based)
            _utc = datetime.now(timezone.utc)
            _mins = _utc.hour * 60 + _utc.minute
            _market_open = _utc.weekday() < 5 and (13 * 60 + 30) <= _mins <= (19 * 60 + 50)
            if not _market_open:
                logger.warning(
                    f"Order for {symbol} skipped — market closed "
                    f"(UTC {_utc.strftime('%H:%M %a')})"
                )
                self.send(
                    "TelegramAgent", "send_message",
                    {"text": f"\u23f0 <b>Order skipped</b>: {symbol} — market closed"},
                )
                return

            # Store pending trade details before placing order
            self._open_trades[symbol] = {
                **payload,
                "filled": False,
                "pending_time": datetime.utcnow().isoformat(),
            }
            logger.info(
                f"Bracket order queued for {symbol}: "
                f"{payload['action']} x{payload['quantity']} @ ${payload['entry_price']:.2f}"
            )
            logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")

            self.send("IBKRClientAgent", "place_bracket_order", {
                "symbol": symbol,
                "action": payload["action"],
                "quantity": payload["quantity"],
                "entry_price": payload["entry_price"],
                "stop_loss": payload["stop_loss"],
                "take_profit": payload["take_profit"],
            })

        elif msg_type == "order_filled":
            fill_type = payload.get("fill_type", "entry")
            if fill_type == "entry":
                await self._on_order_filled(payload)
            elif fill_type == "close":
                # Standalone close (manual market order, not from bracket child)
                await self._on_position_closed(payload)

        elif msg_type == "position_closed":
            # Bracket child (SL or TP) filled
            await self._on_position_closed(payload)

        elif msg_type == "close_position_response":
            symbol = payload.get("symbol")
            success = payload.get("success", False)
            reason = payload.get("reason", "MANUAL")
            if not success:
                logger.warning(
                    f"close_position_market FAILED for {symbol} "
                    f"(IBKR has 0 shares — already closed externally?)"
                )
                trade = self._open_trades.pop(symbol, None)
                if trade:
                    if trade.get("db_id"):
                        await db.update_trade_exit(trade["db_id"], {
                            "exit_price": trade.get("entry_price", 0),
                            "pnl": 0.0,
                            "rr_achieved": 0.0,
                            "hold_minutes": 0.0,
                            "exit_reason": "UNKNOWN_CLOSE",
                            "exit_time": datetime.utcnow().isoformat(),
                        })
                    self.send("RiskAgent", "position_closed_notify", {"symbol": symbol})
                    logger.info(f"Removed {symbol} from _open_trades (UNKNOWN_CLOSE)")
                    logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")
            else:
                logger.info(
                    f"close_position_market sent for {symbol} — waiting for fill confirmation"
                )

        elif msg_type == "ibkr_reconnected":
            await self._restore_positions()

        elif msg_type == "account_response":
            # Reconcile after _restore_positions() requests get_account
            ib_positions = payload.get("open_positions", [])
            ib_syms = {p["symbol"] for p in ib_positions}
            tracked = set(self._open_trades.keys())

            for sym in ib_syms - tracked:
                ib_pos = next(p for p in ib_positions if p["symbol"] == sym)
                logger.warning(
                    f"RESTORE: {sym} open in IBKR (qty={ib_pos['quantity']}) "
                    f"but not tracked — restoring"
                )
                self._open_trades[sym] = {
                    "symbol": sym,
                    "action": "BUY" if ib_pos["quantity"] > 0 else "SELL",
                    "direction": "LONG" if ib_pos["quantity"] > 0 else "SHORT",
                    "quantity": abs(ib_pos["quantity"]),
                    "entry_price": ib_pos.get("avg_cost", 0),
                    "stop_loss": 0,
                    "take_profit": 0,
                    "filled": True,
                    "entry_time": datetime.utcnow().isoformat(),
                    "db_id": None,
                }

            for sym in tracked - ib_syms:
                trade = self._open_trades.get(sym, {})
                if trade.get("filled"):
                    logger.warning(
                        f"RESTORE: {sym} in _open_trades but IBKR has 0 "
                        f"— marking UNKNOWN_OFFLINE"
                    )
                    self._open_trades.pop(sym, None)
                    self.send("RiskAgent", "position_closed_notify", {"symbol": sym})

            if ib_syms or tracked:
                logger.info(
                    f"Position restore complete: "
                    f"IBKR={list(ib_syms)}, tracked_before={list(tracked)}, "
                    f"_open_trades_now={list(self._open_trades.keys())}"
                )

        elif msg_type == "order_rejected":
            symbol = payload.get("symbol")
            if symbol and symbol in self._open_trades:
                trade = self._open_trades[symbol]
                if not trade.get("filled"):
                    del self._open_trades[symbol]
                    logger.info(f"Cleaned up unfilled trade for {symbol} after rejection")
                    logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")

        elif msg_type == "close_position":
            symbol = payload.get("symbol")
            reason = payload.get("reason", "MANUAL")
            if symbol in self._open_trades:
                self._open_trades[symbol]["_closing"] = True
            await self.close_position(symbol, reason)

        elif msg_type == "close_all_positions":
            reason = payload.get("reason", "SHUTDOWN")
            await self.close_all_positions(reason)

        elif msg_type == "market_close":
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
                logger.info(
                    f"Bracket IDs stored for {symbol}: "
                    f"parent={result.get('parent_order_id')}, "
                    f"SL={result.get('sl_order_id')}, "
                    f"TP={result.get('tp_order_id')}"
                )
            elif not result:
                logger.warning(f"Bracket order failed for {symbol} — removing from _open_trades")
                self._open_trades.pop(symbol, None)
                logger.debug(f"_open_trades now: {list(self._open_trades.keys())}")
