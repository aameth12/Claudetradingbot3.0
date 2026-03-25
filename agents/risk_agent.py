"""RiskAgent — Risk rules enforcement for every trade signal."""

import math
from datetime import date, datetime

from loguru import logger

from agents.base_agent import BaseAgent, Message


class RiskAgent(BaseAgent):
    def __init__(self, config: dict, orchestrator=None):
        super().__init__("RiskAgent", orchestrator)
        self.max_daily_loss_pct = config.get("max_daily_loss_pct", 0.10)
        self.max_trade_risk_pct = config.get("max_trade_risk_pct", 0.05)
        self.max_stop_loss_pct = config.get("max_stop_loss_pct", 0.05)
        self.min_rr_ratio = config.get("min_rr_ratio", 2.0)
        self.max_positions = config.get("max_positions", 0)  # 0 = unlimited
        self.allow_shorts = True
        self.allow_longs = True
        self._paused = False

        # State
        self._nav = 10000.0
        self._realized_pnl = 0.0
        self._unrealized_pnl = 0.0
        self._open_positions: dict[str, dict] = {}
        self._daily_loss_halt = False
        self._daily_loss_halt_date: date | None = None

    async def run(self):
        while self._running:
            await self._process_inbox()
            self._check_daily_loss_reset()
            await asyncio.sleep(0.5)

    def _check_daily_loss_reset(self):
        """Reset daily loss halt on new calendar day."""
        if self._daily_loss_halt and self._daily_loss_halt_date:
            if date.today() > self._daily_loss_halt_date:
                self._daily_loss_halt = False
                self._daily_loss_halt_date = None
                logger.info("Daily loss halt reset for new day")

    def validate_signal(self, signal: dict) -> tuple[bool, str, dict]:
        """
        Validate a trade signal against all risk rules.
        Returns (approved, reason, adjusted_signal).
        """
        symbol = signal["symbol"]
        action = signal["action"]
        entry_price = signal.get("entry_price", 0)
        stop_loss = signal.get("stop_loss", 0)
        take_profit = signal.get("take_profit", 0)

        # 1. BOT PAUSED CHECK
        if self._paused:
            return False, "Bot is paused", signal

        # 2. DAILY LOSS HALT
        if self._daily_loss_halt:
            return False, "Daily loss limit reached", signal

        total_pnl = self._realized_pnl + self._unrealized_pnl
        max_loss = self._nav * self.max_daily_loss_pct
        if total_pnl < -max_loss:
            self._daily_loss_halt = True
            self._daily_loss_halt_date = date.today()
            self.broadcast("daily_loss_limit", {
                "nav": self._nav,
                "total_pnl": total_pnl,
                "max_loss": max_loss,
            })
            return False, f"Daily loss limit hit: ${total_pnl:.2f}", signal

        # 3. DIRECTION FILTER
        if action == "BUY" and not self.allow_longs:
            return False, "Long positions disabled", signal
        if action == "SELL" and not self.allow_shorts:
            return False, "Short positions disabled", signal

        # 4. DUPLICATE POSITION
        if symbol in self._open_positions:
            return False, f"Already have open position in {symbol}", signal

        # 5. MAX POSITIONS
        if self.max_positions > 0 and len(self._open_positions) >= self.max_positions:
            return False, f"Max positions reached ({self.max_positions})", signal

        # Validate prices
        if not entry_price or entry_price <= 0:
            return False, "Invalid entry price", signal
        if not stop_loss or stop_loss <= 0:
            return False, "Invalid stop loss", signal
        if not take_profit or take_profit <= 0:
            return False, "Invalid take profit", signal

        # 6. STOP LOSS CLAMP
        sl_dist = abs(entry_price - stop_loss)
        max_sl_dist = entry_price * self.max_stop_loss_pct

        if sl_dist > max_sl_dist:
            if action == "BUY":
                stop_loss = round(entry_price * (1 - self.max_stop_loss_pct), 2)
            else:  # SELL
                stop_loss = round(entry_price * (1 + self.max_stop_loss_pct), 2)
            sl_dist = abs(entry_price - stop_loss)
            logger.info(f"SL clamped to {stop_loss} for {symbol}")

        # 7. MINIMUM R:R ENFORCEMENT
        tp_dist = abs(take_profit - entry_price)
        if sl_dist > 0:
            rr = tp_dist / sl_dist
        else:
            return False, "Stop loss distance is zero", signal

        if rr < self.min_rr_ratio:
            if action == "BUY":
                take_profit = round(entry_price + (self.min_rr_ratio * sl_dist), 2)
            else:  # SELL
                take_profit = round(entry_price - (self.min_rr_ratio * sl_dist), 2)
            tp_dist = abs(take_profit - entry_price)
            rr = tp_dist / sl_dist
            logger.info(f"TP adjusted to {take_profit} for {symbol} (R:R = {rr:.1f})")

        # 8. POSITION SIZING
        max_risk_dollars = self._nav * self.max_trade_risk_pct
        if sl_dist <= 0:
            return False, "Invalid SL distance", signal

        quantity = math.floor(max_risk_dollars / sl_dist)
        if quantity < 1:
            return False, f"Quantity < 1 (risk=${max_risk_dollars:.2f}, sl_dist=${sl_dist:.2f})", signal

        risk_dollars = quantity * sl_dist
        reward_dollars = quantity * tp_dist

        approved_order = {
            **signal,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "quantity": quantity,
            "risk_dollars": round(risk_dollars, 2),
            "reward_dollars": round(reward_dollars, 2),
            "rr_ratio": round(rr, 2),
        }

        return True, "Approved", approved_order

    def get_risk_summary(self) -> dict:
        """Return full risk state for dashboard."""
        return {
            "nav": self._nav,
            "realized_pnl": self._realized_pnl,
            "unrealized_pnl": self._unrealized_pnl,
            "total_pnl": self._realized_pnl + self._unrealized_pnl,
            "daily_loss_halt": self._daily_loss_halt,
            "open_positions_count": len(self._open_positions),
            "max_positions": self.max_positions,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_trade_risk_pct": self.max_trade_risk_pct,
            "min_rr_ratio": self.min_rr_ratio,
            "allow_shorts": self.allow_shorts,
            "allow_longs": self.allow_longs,
            "paused": self._paused,
        }

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "trade_signal":
            approved, reason, adjusted = self.validate_signal(payload)
            if approved:
                logger.info(f"Risk approved: {payload['symbol']} {payload['action']}")
                self.send("ExecutionAgent", "approved_order", adjusted)
            else:
                logger.info(f"Risk rejected: {payload['symbol']} — {reason}")
                self.send("TelegramAgent", "risk_rejection", {
                    "symbol": payload["symbol"],
                    "reason": reason,
                })

        elif msg_type == "account_update":
            self._nav = payload.get("nav", self._nav)
            self._realized_pnl = payload.get("realized_pnl", self._realized_pnl)
            self._unrealized_pnl = payload.get("unrealized_pnl", self._unrealized_pnl)
            positions = payload.get("open_positions", [])
            self._open_positions = {p["symbol"]: p for p in positions}

        elif msg_type == "update_setting":
            key = payload.get("key")
            value = payload.get("value")
            settings_map = {
                "max_daily_loss_pct": ("max_daily_loss_pct", float),
                "max_trade_risk_pct": ("max_trade_risk_pct", float),
                "min_rr_ratio": ("min_rr_ratio", float),
                "max_positions": ("max_positions", int),
                "allow_shorts": ("allow_shorts", lambda v: v in (True, "true", "True", 1)),
                "allow_longs": ("allow_longs", lambda v: v in (True, "true", "True", 1)),
            }
            if key in settings_map:
                attr, converter = settings_map[key]
                setattr(self, attr, converter(value))
                logger.info(f"Risk setting updated: {key} = {getattr(self, attr)}")

        elif msg_type == "pause":
            self._paused = True

        elif msg_type == "resume":
            self._paused = False

        elif msg_type == "position_opened":
            symbol = payload.get("symbol")
            self._open_positions[symbol] = payload

        elif msg_type == "position_closed_notify":
            symbol = payload.get("symbol")
            self._open_positions.pop(symbol, None)


# Required for the run() method
import asyncio
