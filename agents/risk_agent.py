"""RiskAgent — Risk rules enforcement for every trade signal."""

import asyncio
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
        self.max_trades_per_day = config.get("max_trades_per_day", 0)  # 0 = unlimited
        self.max_consecutive_losses = config.get("max_consecutive_losses", 0)  # 0 = disabled
        self.allow_shorts = config.get("allow_shorts", True)
        self.allow_longs = config.get("allow_longs", True)
        self._paused = False

        # State
        self._nav = 10000.0
        self._realized_pnl = 0.0
        self._unrealized_pnl = 0.0
        self._open_positions: dict[str, dict] = {}
        self._daily_loss_halt = False
        self._daily_loss_halt_date: date | None = None
        # PDT day trade counter (same-day round trips)
        self._day_trades_today = 0
        self._day_trades_date: date | None = None
        # Daily trade counter and consecutive loss tracker
        self._trades_today = 0
        self._trades_today_date: date | None = None
        self._consecutive_losses = 0

    async def run(self):
        while self._running:
            await self._process_inbox()
            self._check_daily_loss_reset()
            await asyncio.sleep(0.5)

    def _check_daily_loss_reset(self):
        """Reset daily loss halt and day trade counter on new calendar day."""
        today = date.today()
        if self._daily_loss_halt and self._daily_loss_halt_date:
            if today > self._daily_loss_halt_date:
                self._daily_loss_halt = False
                self._daily_loss_halt_date = None
                logger.info("Daily loss halt reset for new day")
        if self._day_trades_date and today > self._day_trades_date:
            self._day_trades_today = 0
            self._day_trades_date = None
            logger.info("Day trade counter reset for new day")
        if self._trades_today_date and today > self._trades_today_date:
            self._trades_today = 0
            self._trades_today_date = None
            logger.info("Daily trade counter reset for new day")

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

        # 4. MAX TRADES PER DAY
        if self.max_trades_per_day > 0 and self._trades_today >= self.max_trades_per_day:
            return False, f"Max trades per day reached ({self.max_trades_per_day})", signal

        # 5. CONSECUTIVE LOSS LIMIT
        if self.max_consecutive_losses > 0 and self._consecutive_losses >= self.max_consecutive_losses:
            return False, f"Paused: {self._consecutive_losses} consecutive losses", signal

        # 7. DUPLICATE POSITION
        if symbol in self._open_positions:
            return False, f"Already have open position in {symbol}", signal

        # 8. MAX POSITIONS
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

        # Cap total position value (max 40% of NAV per position)
        max_position_value = self._nav * 0.40
        max_qty_by_value = math.floor(max_position_value / entry_price) if entry_price > 0 else 0
        if max_qty_by_value < 1:
            return False, f"Cannot afford even 1 share of {symbol} @ ${entry_price:.2f}", signal
        if quantity > max_qty_by_value:
            quantity = max_qty_by_value
            logger.info(f"Position sized down to {quantity} shares for {symbol} (max 40% NAV = ${max_position_value:,.0f})")

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
            "max_stop_loss_pct": self.max_stop_loss_pct,
            "min_rr_ratio": self.min_rr_ratio,
            "allow_shorts": self.allow_shorts,
            "allow_longs": self.allow_longs,
            "paused": self._paused,
            "day_trades_today": self._day_trades_today,
            "pdt_protected": self._nav < 25000,
            "trades_today": self._trades_today,
            "max_trades_per_day": self.max_trades_per_day,
            "consecutive_losses": self._consecutive_losses,
            "max_consecutive_losses": self.max_consecutive_losses,
        }

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "trade_signal":
            approved, reason, adjusted = self.validate_signal(payload)
            if approved:
                logger.info(f"Risk approved: {payload['symbol']} {payload['action']}")
                self._trades_today += 1
                self._trades_today_date = date.today()
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
            self._consecutive_losses = 0  # Reset on manual resume

        elif msg_type == "position_opened":
            symbol = payload.get("symbol")
            self._open_positions[symbol] = payload

        elif msg_type == "position_closed_notify":
            symbol = payload.get("symbol")
            self._open_positions.pop(symbol, None)
            # Track consecutive losses
            pnl = payload.get("pnl", 0)
            if pnl is not None:
                if pnl < 0:
                    self._consecutive_losses += 1
                    logger.info(f"Consecutive losses: {self._consecutive_losses}")
                    if self.max_consecutive_losses > 0 and self._consecutive_losses >= self.max_consecutive_losses:
                        logger.warning(
                            f"Consecutive loss limit hit ({self._consecutive_losses}). "
                            "New signals paused until manual /resume or new day."
                        )
                        self.send("TelegramAgent", "send_message", {
                            "text": (
                                f"\U0001f6d1 <b>Consecutive loss limit reached</b> ({self._consecutive_losses} losses)\n"
                                "New signals paused. Use /resume to continue or wait for tomorrow."
                            )
                        })
                else:
                    self._consecutive_losses = 0  # Reset on any win

        elif msg_type == "day_trade_completed":
            # Increment day trade counter when a position is opened and closed same day
            self._day_trades_today += 1
            self._day_trades_date = date.today()
            logger.info(f"Day trade #{self._day_trades_today} recorded (NAV=${self._nav:,.0f})")
