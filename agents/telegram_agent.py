"""TelegramAgent — Telegram bot interface, commands, and proactive alerts."""

import asyncio
import os
import subprocess
import sys

from loguru import logger
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from agents.base_agent import BaseAgent, Message
from db import database as db
from utils.helpers import format_currency, format_pct, is_market_open, next_market_open, pnl_emoji


class TelegramAgent(BaseAgent):
    def __init__(self, config: dict, orchestrator=None):
        super().__init__("TelegramAgent", orchestrator)
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.app: Application | None = None
        self._account_data: dict = {}
        self._positions_data: list[dict] = []
        self._risk_data: dict = {}
        self._market_data: dict = {}

    async def run(self):
        """Start the Telegram bot and process inbox."""
        if not self.token:
            logger.warning("No TELEGRAM_BOT_TOKEN set, TelegramAgent disabled")
            while self._running:
                await self._process_inbox()
                await asyncio.sleep(1)
            return

        self.app = Application.builder().token(self.token).build()
        self._register_handlers()

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)

        logger.info("Telegram bot started")

        while self._running:
            await self._process_inbox()
            await asyncio.sleep(0.5)

        await self.app.updater.stop()
        await self.app.stop()
        await self.app.shutdown()

    def _register_handlers(self):
        """Register all command handlers."""
        commands = {
            "start": self._cmd_start,
            "status": self._cmd_status,
            "positions": self._cmd_positions,
            "pnl": self._cmd_pnl,
            "risk": self._cmd_risk,
            "history": self._cmd_history,
            "performance": self._cmd_performance,
            "watchlist": self._cmd_watchlist,
            "add": self._cmd_add,
            "remove": self._cmd_remove,
            "data": self._cmd_data,
            "set": self._cmd_set,
            "stop": self._cmd_stop,
            "resume": self._cmd_resume,
            "kill": self._cmd_kill,
            "help": self._cmd_help,
            "update": self._cmd_update,
        }
        for name, handler in commands.items():
            self.app.add_handler(CommandHandler(name, handler))

    def _auth(self, update: Update) -> bool:
        """Check if the message is from the authorized chat."""
        return str(update.effective_chat.id) == self.chat_id

    async def _send(self, text: str, chat_id: str | None = None):
        """Send a message to the authorized chat."""
        if not self.app:
            return
        target = chat_id or self.chat_id
        if not target:
            return
        try:
            await self.app.bot.send_message(
                chat_id=target,
                text=text,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")

    # ─── Commands ─────────────────────────────────────────────────────

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        text = (
            "<b>AI Trading Bot Commands</b>\n"
            "/help — Detailed guide for all commands\n"
            "/status — Full live dashboard\n"
            "/positions — Open positions\n"
            "/pnl — P&L breakdown\n"
            "/risk — Risk exposure\n"
            "/history — Last 20 trades\n"
            "/performance — 7-day table\n"
            "/watchlist — Current symbols\n"
            "/add SYMBOL — Add to watchlist\n"
            "/remove SYMBOL — Remove from watchlist\n"
            "/data SYMBOL — Live price + scan\n"
            "/set key value — Tune settings\n"
            "/stop — Pause trading\n"
            "/resume — Resume trading\n"
            "/kill — Emergency shutdown\n"
            "/update — Pull latest code from GitHub"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        # Split into multiple messages to stay under Telegram's 4096 char limit
        msg1 = (
            "<b>AI Trading Bot — Command Guide (1/3)</b>\n\n"
            "<b>/start</b> — Quick command list\n\n"
            "<b>/help</b> — This detailed guide\n\n"
            "<b>/status</b> — Full dashboard: market status, "
            "balance, today's trades & P&L, all-time stats, "
            "and open positions.\n\n"
            "<b>/positions</b> — Open positions with entry "
            "price, current price, P&L, SL & TP levels.\n\n"
            "<b>/pnl</b> — Today's and all-time P&L.\n\n"
            "<b>/risk</b> — Risk dashboard: NAV, daily loss "
            "halt, position count, max loss %, max trade "
            "risk %, min R:R, longs/shorts status.\n\n"
            "<b>/history</b> — Last 20 trades with P&L "
            "and exit reason.\n\n"
            "<b>/performance</b> — 7-day table with trades, "
            "wins, and P&L per day."
        )

        msg2 = (
            "<b>Command Guide (2/3)</b>\n\n"
            "<b>/watchlist</b> — Current symbols being scanned.\n\n"
            "<b>/add SYMBOL</b> — Add ticker to watchlist.\n"
            "Example: <code>/add PLTR</code>\n\n"
            "<b>/remove SYMBOL</b> — Remove ticker. Existing "
            "positions are NOT closed.\n"
            "Example: <code>/remove TSLA</code>\n\n"
            "<b>/data SYMBOL</b> — Live price + instant AI scan.\n"
            "Example: <code>/data AAPL</code>\n\n"
            "<b>/set key value</b> — Change settings live:\n"
            "  <code>confidence</code> — AI threshold (0-1)\n"
            "  <code>max_daily_loss</code> — daily loss % (0-1)\n"
            "  <code>max_trade_risk</code> — per-trade risk %\n"
            "  <code>min_rr</code> — reward:risk ratio\n"
            "  <code>max_positions</code> — max open (0=unlimited)\n"
            "  <code>allow_shorts</code> — true/false\n"
            "  <code>allow_longs</code> — true/false\n"
            "Example: <code>/set confidence 0.75</code>\n\n"
            "<b>/stop</b> — Pause trading. Bot stays connected, "
            "existing positions keep SL/TP.\n\n"
            "<b>/resume</b> — Resume after pause.\n\n"
            "<b>/kill</b> — Emergency: closes ALL positions "
            "and shuts down the bot.\n\n"
            "<b>/update</b> — Pull latest code from GitHub "
            "and restart the bot with new changes."
        )

        msg3 = (
            "<b>Command Guide (3/3)</b>\n\n"
            "<b>When the market is closed:</b>\n"
            "- Bot stays connected to IB Gateway\n"
            "- Heartbeat runs every 60s monitoring health\n"
            "- Does NOT scan or place orders\n"
            "- Telegram commands stay active\n"
            "- Auto-detects market open and starts trading\n"
            "- Sends daily summary at market close\n\n"
            "You do NOT need to restart each day — "
            "just leave it running."
        )

        await update.message.reply_text(msg1, parse_mode="HTML")
        await update.message.reply_text(msg2, parse_mode="HTML")
        await update.message.reply_text(msg3, parse_mode="HTML")

    async def _cmd_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        await update.message.reply_text(
            "\U0001f504 Pulling latest code from GitHub...", parse_mode="HTML"
        )
        try:
            # Run git pull in the bot's directory
            bot_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            result = subprocess.run(
                ["git", "pull", "origin", "main"],
                cwd=bot_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            output = result.stdout.strip() or result.stderr.strip()
            if result.returncode == 0:
                if "Already up to date" in output:
                    await update.message.reply_text(
                        "\u2705 Already up to date. No changes.", parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text(
                        f"\u2705 Updated!\n<pre>{output[:1000]}</pre>\n\n"
                        "\U0001f504 Restarting bot...",
                        parse_mode="HTML",
                    )
                    # Restart the bot process
                    os.execv(sys.executable, [sys.executable] + sys.argv)
            else:
                await update.message.reply_text(
                    f"\u274c Update failed:\n<pre>{output[:1000]}</pre>",
                    parse_mode="HTML",
                )
        except Exception as e:
            await update.message.reply_text(
                f"\u274c Update error: {e}", parse_mode="HTML"
            )

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        market_status = "\U0001f7e2 OPEN" if is_market_open() else "\U0001f534 CLOSED"
        nav = self._account_data.get("nav", 0)
        total_pnl = self._account_data.get("realized_pnl", 0) + self._account_data.get("unrealized_pnl", 0)
        paused = self._risk_data.get("paused", False)
        mode_str = "\u23f8 PAUSED" if paused else "\u25b6 RUNNING"

        # Today's trades
        today_trades = await db.get_todays_trades()
        today_count = len(today_trades)
        today_wins = sum(1 for t in today_trades if t.get("pnl") and t["pnl"] > 0)
        today_pnl = sum(t.get("pnl", 0) for t in today_trades if t.get("pnl"))

        # All-time
        all_trades = await db.get_all_trades()
        all_count = len(all_trades)
        all_wins = sum(1 for t in all_trades if t.get("pnl") and t["pnl"] > 0)
        all_win_rate = (all_wins / all_count * 100) if all_count else 0
        all_pnl = sum(t.get("pnl", 0) for t in all_trades)
        gross_profit = sum(t["pnl"] for t in all_trades if t.get("pnl") and t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in all_trades if t.get("pnl") and t["pnl"] < 0))
        profit_factor = (gross_profit / gross_loss) if gross_loss else 0

        text = f"<b>Dashboard</b>\n{'=' * 30}\n"
        text += f"Market: {market_status}\n"

        if not is_market_open():
            nxt = next_market_open()
            if nxt:
                text += f"Next Open: {nxt.strftime('%a %b %d, %I:%M %p ET')}\n"

        text += f"Mode: paper | Engine: {mode_str}\n"
        text += f"<b>Account:</b>\n"
        text += f"  Balance: ${nav:,.0f}\n"
        text += f"  Total P&L: {format_currency(total_pnl)}\n"
        text += f"<b>Today</b>\n{'-' * 30}\n"
        text += f"Trades: {today_count} | Wins: {today_wins} | P&L: {format_currency(today_pnl)}\n"
        text += f"<b>All-Time (Bot Tracked)</b>\n{'-' * 30}\n"
        text += f"Total Trades: {all_count} | Win Rate: {all_win_rate:.1f}%\n"
        text += f"Total P&L: {format_currency(all_pnl)}\n"
        text += f"Profit Factor: {profit_factor:.2f}\n"

        # Open positions
        if self._positions_data:
            text += f"<b>Open Positions ({len(self._positions_data)})</b>\n{'-' * 30}\n"
            total_unrealized = 0
            for p in self._positions_data:
                pnl = p.get("pnl", 0)
                pnl_pct = p.get("pnl_pct", 0)
                total_unrealized += pnl
                text += (
                    f"  {p['direction']} {p['symbol']} x{p['quantity']} @ ${p['entry_price']:.2f}\n"
                    f"    Now: ${p.get('current_price', 0):.2f} | P&L: {format_currency(pnl)} ({format_pct(pnl_pct)})\n"
                )
            text += f"  Total Unrealized: {format_currency(total_unrealized)}\n"

        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        if not self._positions_data:
            await update.message.reply_text("No open positions.", parse_mode="HTML")
            return

        text = "<b>Open Positions</b>\n"
        for p in self._positions_data:
            text += (
                f"\n{p['direction']} <b>{p['symbol']}</b> x{p['quantity']}\n"
                f"Entry: ${p['entry_price']:.2f} | Now: ${p.get('current_price', 0):.2f}\n"
                f"P&L: {format_currency(p.get('pnl', 0))} ({format_pct(p.get('pnl_pct', 0))})\n"
                f"SL: ${p.get('stop_loss', 0):.2f} | TP: ${p.get('take_profit', 0):.2f}\n"
            )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        today_trades = await db.get_todays_trades()
        today_pnl = sum(t.get("pnl", 0) for t in today_trades if t.get("pnl"))

        all_trades = await db.get_all_trades()
        all_pnl = sum(t.get("pnl", 0) for t in all_trades)

        text = (
            f"<b>P&L Summary</b>\n"
            f"Today: {format_currency(today_pnl)}\n"
            f"All-Time: {format_currency(all_pnl)}\n"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_risk(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        r = self._risk_data
        text = (
            f"<b>Risk Dashboard</b>\n"
            f"NAV: ${r.get('nav', 0):,.0f}\n"
            f"Daily Loss Halt: {'YES \U0001f6d1' if r.get('daily_loss_halt') else 'No'}\n"
            f"Open Positions: {r.get('open_positions_count', 0)}"
            + (f" / {r.get('max_positions', 0)}" if r.get('max_positions', 0) > 0 else "")
            + "\n"
            f"Max Daily Loss: {r.get('max_daily_loss_pct', 0) * 100:.0f}%\n"
            f"Max Trade Risk: {r.get('max_trade_risk_pct', 0) * 100:.0f}%\n"
            f"Min R:R: {r.get('min_rr_ratio', 0):.1f}\n"
            f"Longs: {'ON' if r.get('allow_longs') else 'OFF'} | "
            f"Shorts: {'ON' if r.get('allow_shorts') else 'OFF'}\n"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        trades = await db.get_recent_trades(20)
        if not trades:
            await update.message.reply_text("No trade history yet.", parse_mode="HTML")
            return

        text = "<b>Last 20 Trades</b>\n"
        for t in trades:
            emoji = pnl_emoji(t.get("pnl", 0))
            text += (
                f"{emoji} {t['symbol']} {t['action']} x{t['quantity']} "
                f"| {format_currency(t.get('pnl', 0))} | {t.get('exit_reason', '?')}\n"
            )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_performance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        perfs = await db.get_daily_performance(7)
        if not perfs:
            await update.message.reply_text("No performance data yet.", parse_mode="HTML")
            return

        text = "<b>7-Day Performance</b>\n"
        text += f"{'Date':<12}{'Trades':>7}{'Wins':>6}{'P&L':>10}\n"
        for p in perfs:
            text += (
                f"{p['date']:<12}{p.get('total_trades', 0):>7}"
                f"{p.get('wins', 0):>6}{format_currency(p.get('gross_pnl', 0)):>10}\n"
            )
        await update.message.reply_text(f"<pre>{text}</pre>", parse_mode="HTML")

    async def _cmd_watchlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        # Get watchlist from orchestrator config
        watchlist = getattr(self.orchestrator, "watchlist", []) if self.orchestrator else []
        text = f"<b>Watchlist ({len(watchlist)} symbols)</b>\n{', '.join(watchlist)}"
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /add SYMBOL", parse_mode="HTML")
            return
        symbol = context.args[0].upper()
        self.broadcast("watchlist_add", {"symbol": symbol})
        await update.message.reply_text(f"Added {symbol} to watchlist", parse_mode="HTML")

    async def _cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /remove SYMBOL", parse_mode="HTML")
            return
        symbol = context.args[0].upper()
        self.broadcast("watchlist_remove", {"symbol": symbol})
        await update.message.reply_text(f"Removed {symbol} from watchlist", parse_mode="HTML")

    async def _cmd_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /data SYMBOL", parse_mode="HTML")
            return
        symbol = context.args[0].upper()
        self.send("IBKRClientAgent", "get_snapshot", {"symbol": symbol})
        self.send("StrategyAgent", "force_scan", {"symbol": symbol})
        await update.message.reply_text(f"Fetching data for {symbol}...", parse_mode="HTML")

    async def _cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /set <key> <value>\n"
                "Keys: confidence, max_daily_loss, max_trade_risk, "
                "min_rr, max_positions, allow_shorts, allow_longs",
                parse_mode="HTML",
            )
            return

        key = context.args[0].lower()
        value = context.args[1]

        setting_map = {
            "confidence": ("confidence_threshold", "StrategyAgent"),
            "max_daily_loss": ("max_daily_loss_pct", "RiskAgent"),
            "max_trade_risk": ("max_trade_risk_pct", "RiskAgent"),
            "min_rr": ("min_rr_ratio", "RiskAgent"),
            "max_positions": ("max_positions", "RiskAgent"),
            "allow_shorts": ("allow_shorts", "RiskAgent"),
            "allow_longs": ("allow_longs", "RiskAgent"),
        }

        if key not in setting_map:
            await update.message.reply_text(f"Unknown setting: {key}", parse_mode="HTML")
            return

        setting_key, target_agent = setting_map[key]
        self.send(target_agent, "update_setting", {"key": setting_key, "value": value})

        # Also send to StrategyAgent for direction settings
        if key in ("allow_shorts", "allow_longs"):
            self.send("StrategyAgent", "update_setting", {"key": key, "value": value})

        await update.message.reply_text(f"Set {key} = {value}", parse_mode="HTML")

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self.broadcast("pause", {})
        await update.message.reply_text("\u23f8 Trading paused", parse_mode="HTML")

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self.broadcast("resume", {})
        await update.message.reply_text("\u25b6 Trading resumed", parse_mode="HTML")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        await update.message.reply_text(
            "\U0001f6a8 Emergency shutdown initiated — closing all positions...",
            parse_mode="HTML",
        )
        self.send("ExecutionAgent", "close_all_positions", {"reason": "SHUTDOWN"})
        # Signal shutdown
        import os, signal
        os.kill(os.getpid(), signal.SIGTERM)

    # ─── Message Handling ─────────────────────────────────────────────

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "send_message":
            await self._send(payload.get("text", ""))

        elif msg_type == "account_update":
            self._account_data = payload

        elif msg_type == "market_data_update":
            self._market_data.update(payload)

        elif msg_type == "positions_summary_response":
            self._positions_data = payload.get("positions", [])

        elif msg_type == "risk_update":
            self._risk_data = payload

        elif msg_type == "daily_loss_limit":
            await self._send(
                f"\U0001f6a8 <b>DAILY LOSS LIMIT REACHED</b>\n"
                f"NAV: ${payload.get('nav', 0):,.0f}\n"
                f"Total P&L: {format_currency(payload.get('total_pnl', 0))}\n"
                f"Max Loss: ${payload.get('max_loss', 0):,.0f}\n"
                f"Trading halted until tomorrow."
            )

        elif msg_type == "market_open":
            await self._send("\U0001f514 <b>MARKET OPEN</b>\nTrading is active.")

        elif msg_type == "market_close":
            # Build daily summary
            today_trades = await db.get_todays_trades()
            today_pnl = sum(t.get("pnl", 0) for t in today_trades if t.get("pnl"))
            today_wins = sum(1 for t in today_trades if t.get("pnl") and t["pnl"] > 0)
            await self._send(
                f"\U0001f515 <b>MARKET CLOSED</b>\n"
                f"Today: {len(today_trades)} trades | {today_wins} wins\n"
                f"P&L: {format_currency(today_pnl)}"
            )

        elif msg_type == "ibkr_reconnected":
            await self._send("\u2705 <b>IB Gateway reconnected</b>")

        elif msg_type == "ibkr_disconnect_fatal":
            await self._send(
                f"\U0001f6a8 <b>IB Gateway FATAL DISCONNECT</b>\n"
                f"Reason: {payload.get('reason', 'Unknown')}"
            )

        elif msg_type == "goal_hit":
            target = payload.get("target", 0)
            next_target = payload.get("next_target", 0)
            await self._send(
                f"\U0001f3c6 <b>DAILY GOAL REACHED</b>\n"
                f"Target: ${target:.2f} \u2705\n"
                f"Next target: ${next_target:.2f}"
            )

        elif msg_type == "confidence_adjusted":
            old_val = payload.get("old", 0)
            new_val = payload.get("new", 0)
            reason = payload.get("reason", "")
            await self._send(
                f"\U0001f527 <b>Confidence threshold adjusted</b>\n"
                f"{old_val:.2f} \u2192 {new_val:.2f}\n"
                f"Reason: {reason}"
            )

        elif msg_type == "snapshot_response":
            data = payload.get("data")
            symbol = payload.get("symbol")
            if data:
                await self._send(
                    f"<b>{symbol} Live Data</b>\n"
                    f"Bid: ${data.get('bid', 'N/A')} | Ask: ${data.get('ask', 'N/A')}\n"
                    f"Last: ${data.get('last', 'N/A')} | Vol: {data.get('volume', 'N/A')}\n"
                    f"High: ${data.get('high', 'N/A')} | Low: ${data.get('low', 'N/A')}"
                )

        elif msg_type == "risk_rejection":
            # Silent — don't spam the user with rejections
            pass

    async def stop(self):
        """Send shutdown notification before stopping."""
        await self._send("\u26a0\ufe0f Bot shutting down...")
        await super().stop()
