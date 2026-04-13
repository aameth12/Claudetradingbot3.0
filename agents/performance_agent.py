"""PerformanceAgent — Metrics tracking, goal system, self-optimisation."""

import asyncio
from datetime import datetime, date

import yaml
from loguru import logger

from agents.base_agent import BaseAgent, Message
from db import database as db


class PerformanceAgent(BaseAgent):
    def __init__(self, config: dict, orchestrator=None):
        super().__init__("PerformanceAgent", orchestrator)
        self.daily_pnl_target = config.get("daily_pnl_target", 100.0)
        self.goal_escalation_pct = config.get("goal_escalation_pct", 0.05)
        self.cool_down_minutes = config.get("cool_down_minutes", 120)
        self.cool_down_win_rate_threshold = config.get("cool_down_win_rate_threshold", 0.30)
        self._cool_downs: dict[str, datetime] = {}  # symbol -> cool_down_until
        self._analysis_interval = 300  # 5 minutes

    async def run(self):
        """Run analysis periodically."""
        while self._running:
            await self._process_inbox()
            await asyncio.sleep(self._analysis_interval)
            await self._run_analysis()

    async def _run_analysis(self):
        """Run full performance analysis."""
        try:
            today_trades = await db.get_todays_trades()
            closed_today = [t for t in today_trades if t.get("exit_price") is not None]

            if not closed_today:
                return

            wins = [t for t in closed_today if t.get("pnl", 0) > 0]
            losses = [t for t in closed_today if t.get("pnl", 0) <= 0]

            total_trades = len(closed_today)
            win_count = len(wins)
            loss_count = len(losses)
            win_rate = win_count / total_trades if total_trades else 0

            gross_pnl = sum(t.get("pnl", 0) for t in closed_today)
            avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
            avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0

            rr_values = [t.get("rr_achieved", 0) for t in closed_today if t.get("rr_achieved")]
            avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0

            biggest_win = max((t.get("pnl", 0) for t in closed_today), default=0)
            biggest_loss = min((t.get("pnl", 0) for t in closed_today), default=0)

            # Check goal
            goal_hit = gross_pnl >= self.daily_pnl_target

            # Save to DB
            await db.upsert_daily_performance({
                "date": date.today().isoformat(),
                "total_trades": total_trades,
                "wins": win_count,
                "losses": loss_count,
                "win_rate": round(win_rate, 4),
                "gross_pnl": round(gross_pnl, 2),
                "avg_win": round(avg_win, 2),
                "avg_loss": round(avg_loss, 2),
                "avg_rr": round(avg_rr, 2),
                "biggest_win": round(biggest_win, 2),
                "biggest_loss": round(biggest_loss, 2),
                "goal_target": self.daily_pnl_target,
                "goal_hit": 1 if goal_hit else 0,
            })

            if goal_hit:
                next_target = self.daily_pnl_target * (1 + self.goal_escalation_pct)
                self.send("TelegramAgent", "goal_hit", {
                    "target": self.daily_pnl_target,
                    "next_target": next_target,
                })
                self.daily_pnl_target = next_target

            # Self-optimisation: confidence threshold adjustment
            if total_trades >= 5:
                await self._adjust_confidence(win_rate)

            # Symbol cool-down check
            await self._check_symbol_cooldowns()

        except Exception as e:
            logger.exception(f"Error in performance analysis: {e}")

    async def _adjust_confidence(self, win_rate: float):
        """Auto-adjust confidence threshold based on win rate."""
        try:
            with open("config.yaml", "r") as f:
                config = yaml.safe_load(f) or {}

            current = config.get("strategy", {}).get("confidence_threshold", 0.65)
            new_threshold = current

            if win_rate < 0.40:
                new_threshold = min(current + 0.05, 0.85)
                reason = f"Win rate {win_rate:.0%} < 40%"
            elif win_rate > 0.70:
                new_threshold = max(current - 0.05, 0.50)
                reason = f"Win rate {win_rate:.0%} > 70%"
            else:
                return

            if new_threshold != current:
                config.setdefault("strategy", {})["confidence_threshold"] = new_threshold
                with open("config.yaml", "w") as f:
                    yaml.dump(config, f, default_flow_style=False)

                self.send("StrategyAgent", "update_setting", {
                    "key": "confidence_threshold",
                    "value": new_threshold,
                })
                self.send("TelegramAgent", "confidence_adjusted", {
                    "old": current,
                    "new": new_threshold,
                    "reason": reason,
                })
                logger.info(f"Confidence threshold adjusted: {current} -> {new_threshold} ({reason})")

        except Exception as e:
            logger.warning(f"Error adjusting confidence: {e}")

    async def _check_symbol_cooldowns(self):
        """Check per-symbol win rates and add cool-downs for poor performers."""
        try:
            all_trades = await db.get_all_trades()

            # Group by symbol
            symbol_trades: dict[str, list] = {}
            for t in all_trades:
                sym = t["symbol"]
                if sym not in symbol_trades:
                    symbol_trades[sym] = []
                symbol_trades[sym].append(t)

            now = datetime.utcnow()

            for symbol, trades in symbol_trades.items():
                recent = trades[:100]  # Most recent (already sorted DESC)
                if len(recent) < 5:
                    continue

                wins = sum(1 for t in recent if t.get("pnl", 0) > 0)
                sym_win_rate = wins / len(recent)

                if sym_win_rate < self.cool_down_win_rate_threshold:
                    from datetime import timedelta
                    cool_until = now + timedelta(minutes=self.cool_down_minutes)
                    self._cool_downs[symbol] = cool_until
                    logger.info(
                        f"Symbol {symbol} cooled down until {cool_until} "
                        f"(win rate: {sym_win_rate:.0%} over {len(recent)} trades)"
                    )

            # Clean expired cool-downs
            expired = [s for s, t in self._cool_downs.items() if t < now]
            for s in expired:
                del self._cool_downs[s]

        except Exception as e:
            logger.warning(f"Error checking symbol cooldowns: {e}")

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "market_close":
            await self._run_analysis()

        elif msg_type == "check_cool_down":
            symbol = payload.get("symbol")
            now = datetime.utcnow()
            is_cooled = False
            if symbol in self._cool_downs:
                if self._cool_downs[symbol] > now:
                    is_cooled = True
                else:
                    del self._cool_downs[symbol]

            self.send(message.sender, "cool_down_response", {
                "symbol": symbol,
                "cooled_down": is_cooled,
            })
