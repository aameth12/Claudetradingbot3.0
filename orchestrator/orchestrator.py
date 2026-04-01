"""Orchestrator — Message bus, heartbeat, position poller, state integrity checker."""

import asyncio
import signal
import sys
from datetime import datetime

import yaml
from loguru import logger

from agents.base_agent import BaseAgent, Message
from agents.ibkr_client_agent import IBKRClientAgent
from agents.data_agent import DataAgent
from agents.strategy_agent import StrategyAgent
from agents.risk_agent import RiskAgent
from agents.execution_agent import ExecutionAgent
from agents.telegram_agent import TelegramAgent
from agents.performance_agent import PerformanceAgent
from db import database as db
from utils.helpers import is_market_open


class Orchestrator:
    def __init__(self, config: dict):
        self.config = config
        self.watchlist = list(config.get("watchlist", []))
        self._was_market_open = False
        self._running = False

        # Instantiate all agents
        self.ibkr_agent = IBKRClientAgent(config.get("ibkr", {}), orchestrator=self)
        self.data_agent = DataAgent(config.get("data", {}), self.watchlist, orchestrator=self)
        self.strategy_agent = StrategyAgent(config.get("strategy", {}), self.watchlist, orchestrator=self)
        self.risk_agent = RiskAgent(config.get("risk", {}), orchestrator=self)
        self.execution_agent = ExecutionAgent(config.get("risk", {}), orchestrator=self)
        self.telegram_agent = TelegramAgent(config.get("telegram", {}), orchestrator=self)
        self.performance_agent = PerformanceAgent(config.get("performance", {}), orchestrator=self)

        # Merge strategy direction filters into RiskAgent
        # (they live under strategy: in config.yaml but RiskAgent only receives risk: section)
        strategy_cfg = config.get("strategy", {})
        self.risk_agent.allow_shorts = strategy_cfg.get("allow_shorts", True)
        self.risk_agent.allow_longs = strategy_cfg.get("allow_longs", True)
        logger.info(
            f"Strategy config: allow_longs={self.risk_agent.allow_longs}, "
            f"allow_shorts={self.risk_agent.allow_shorts}"
        )

        self.agents: dict[str, BaseAgent] = {
            "IBKRClientAgent": self.ibkr_agent,
            "DataAgent": self.data_agent,
            "StrategyAgent": self.strategy_agent,
            "RiskAgent": self.risk_agent,
            "ExecutionAgent": self.execution_agent,
            "TelegramAgent": self.telegram_agent,
            "PerformanceAgent": self.performance_agent,
        }

        self._tasks: list[asyncio.Task] = []

    async def dispatch(self, message: Message):
        """Route a message to its recipient agent or broadcast to all."""
        if message.recipient == "broadcast":
            for name, agent in self.agents.items():
                if name != message.sender:
                    await agent.inbox.put(message)
        else:
            agent = self.agents.get(message.recipient)
            if agent:
                await agent.inbox.put(message)
            else:
                logger.warning(f"Unknown recipient: {message.recipient}")

    async def start(self):
        """Start all agents and background tasks."""
        self._running = True

        # Initialize database
        await db.init_db()

        # Start all agent tasks
        for name, agent in self.agents.items():
            task = asyncio.create_task(agent.start(), name=f"agent_{name}")
            self._tasks.append(task)
            logger.info(f"Started agent: {name}")

        # Subscribe to market data after IBKR connects
        await asyncio.sleep(5)
        await self.ibkr_agent.subscribe_market_data(self.watchlist)

        # Start background tasks
        self._tasks.append(asyncio.create_task(self._heartbeat(), name="heartbeat"))
        self._tasks.append(asyncio.create_task(self._position_poller(), name="position_poller"))
        self._tasks.append(asyncio.create_task(self._state_integrity_checker(), name="state_checker"))

        # Set up signal handlers (Windows-safe)
        try:
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self._shutdown()))
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self._shutdown()))
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

        logger.info("Orchestrator started — all agents running")

        # Wait for all tasks
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    async def _heartbeat(self):
        """60-second heartbeat loop: fires market_open/close events."""
        while self._running:
            try:
                currently_open = is_market_open()

                if currently_open and not self._was_market_open:
                    logger.info("Market OPENED")
                    msg = Message(
                        sender="Orchestrator",
                        recipient="broadcast",
                        type="market_open",
                        payload={},
                    )
                    await self.dispatch(msg)
                    self._was_market_open = True

                elif not currently_open and self._was_market_open:
                    logger.info("Market CLOSED")
                    msg = Message(
                        sender="Orchestrator",
                        recipient="broadcast",
                        type="market_close",
                        payload={},
                    )
                    await self.dispatch(msg)
                    self._was_market_open = False

            except Exception as e:
                logger.warning(f"Heartbeat error: {e}")

            await asyncio.sleep(60)

    async def _position_poller(self):
        """30-second loop: push live position P&L, reconcile IBKR vs _open_trades."""
        while self._running:
            await asyncio.sleep(30)
            try:
                # Get market data snapshots for open positions
                market_data = {}
                for symbol in list(self.execution_agent._open_trades.keys()):
                    snapshot = self.ibkr_agent.get_snapshot(symbol)
                    if snapshot:
                        market_data[symbol] = snapshot

                # Get positions summary
                positions = self.execution_agent.get_open_positions_summary(market_data)

                # Get risk summary
                risk_summary = self.risk_agent.get_risk_summary()

                # Push to TelegramAgent
                await self.telegram_agent.inbox.put(Message(
                    sender="Orchestrator",
                    recipient="TelegramAgent",
                    type="positions_summary_response",
                    payload={"positions": positions},
                ))

                await self.telegram_agent.inbox.put(Message(
                    sender="Orchestrator",
                    recipient="TelegramAgent",
                    type="risk_update",
                    payload=risk_summary,
                ))

                # ── IBKR Position Reconciler ──────────────────────────────────
                # Compare live IBKR positions with _open_trades to catch
                # externally-opened or externally-closed positions.
                try:
                    ib_positions = {
                        pos.contract.symbol: pos
                        for pos in self.ibkr_agent.ib.positions()
                        if pos.position != 0
                    }
                    tracked = set(self.execution_agent._open_trades.keys())
                    ib_syms = set(ib_positions.keys())

                    # IBKR has a position the bot isn't tracking → restore it
                    for sym in ib_syms - tracked:
                        pos = ib_positions[sym]
                        qty = int(pos.position)
                        logger.warning(
                            f"RECONCILER: {sym} open in IBKR (qty={qty}) "
                            f"but missing from _open_trades — restoring"
                        )
                        self.execution_agent._open_trades[sym] = {
                            "symbol": sym,
                            "action": "BUY" if qty > 0 else "SELL",
                            "direction": "LONG" if qty > 0 else "SHORT",
                            "quantity": abs(qty),
                            "entry_price": pos.avgCost,
                            "stop_loss": 0,
                            "take_profit": 0,
                            "filled": True,
                            "entry_time": datetime.utcnow().isoformat(),
                            "db_id": None,
                        }
                        await self.telegram_agent.inbox.put(Message(
                            sender="Orchestrator",
                            recipient="TelegramAgent",
                            type="send_message",
                            payload={
                                "text": (
                                    f"\u26a0\ufe0f <b>Untracked position restored</b>\n"
                                    f"{sym} {'LONG' if qty > 0 else 'SHORT'} x{abs(qty)} "
                                    f"@ ${pos.avgCost:.2f} (from IBKR)"
                                )
                            },
                        ))

                    # Bot is tracking a position IBKR doesn't show → clean up
                    for sym in tracked - ib_syms:
                        trade = self.execution_agent._open_trades.get(sym, {})
                        if trade.get("filled"):
                            logger.warning(
                                f"RECONCILER: {sym} in _open_trades but IBKR shows 0 "
                                f"— removing as UNKNOWN_CLOSE"
                            )
                            await self.execution_agent.inbox.put(Message(
                                sender="Orchestrator",
                                recipient="ExecutionAgent",
                                type="close_position_response",
                                payload={"symbol": sym, "success": False, "reason": "UNKNOWN_CLOSE"},
                            ))
                except Exception as e:
                    logger.warning(f"Reconciler error: {e}")

            except Exception as e:
                logger.warning(f"Position poller error: {e}")

    async def _state_integrity_checker(self):
        """
        Every 60s: cross-check all internal state, log a health summary,
        and send a Telegram alert when state diverges.
        """
        while self._running:
            await asyncio.sleep(60)
            try:
                open_trades = dict(self.execution_agent._open_trades)
                risk_positions = dict(self.risk_agent._open_positions)
                ib_positions = {
                    pos.contract.symbol: pos
                    for pos in self.ibkr_agent.ib.positions()
                    if pos.position != 0
                }
                nav = self.risk_agent._nav
                bracket_count = len(self.ibkr_agent._bracket_groups)
                connected = self.ibkr_agent._connected

                logger.info(
                    f"[STATE] connected={connected} | NAV=${nav:,.0f} | "
                    f"_open_trades={list(open_trades.keys())} | "
                    f"risk_positions={list(risk_positions.keys())} | "
                    f"IBKR_positions={list(ib_positions.keys())} | "
                    f"bracket_groups={bracket_count}"
                )

                alerts = []
                if not connected:
                    alerts.append("Not connected to IB Gateway")
                if nav <= 0:
                    alerts.append("NAV=$0 — account_update not receiving data")
                for sym in open_trades:
                    if open_trades[sym].get("filled") and sym not in ib_positions:
                        alerts.append(f"{sym} in _open_trades but IBKR=0 (ghost position)")
                for sym in ib_positions:
                    if sym not in open_trades:
                        alerts.append(f"{sym} open in IBKR but not tracked")

                if alerts:
                    alert_text = "\u26a0\ufe0f <b>State Mismatch Detected</b>\n" + "\n".join(
                        f"• {a}" for a in alerts
                    )
                    await self.telegram_agent.inbox.put(Message(
                        sender="Orchestrator",
                        recipient="TelegramAgent",
                        type="send_message",
                        payload={"text": alert_text},
                    ))
            except Exception as e:
                logger.warning(f"State checker error: {e}")

    async def _shutdown(self):
        """Graceful shutdown: close positions, notify, stop agents."""
        logger.info("Shutdown initiated")
        self._running = False

        try:
            # Close all positions
            await self.execution_agent.close_all_positions("SHUTDOWN")
            await asyncio.sleep(2)

            # Notify Telegram
            await self.telegram_agent.inbox.put(Message(
                sender="Orchestrator",
                recipient="TelegramAgent",
                type="send_message",
                payload={"text": "\u26a0\ufe0f Bot shutdown complete."},
            ))
            await asyncio.sleep(1)

        except Exception as e:
            logger.warning(f"Error during shutdown: {e}")

        # Stop all agents
        for name, agent in self.agents.items():
            try:
                await agent.stop()
            except Exception as e:
                logger.warning(f"Error stopping {name}: {e}")

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        logger.info("Shutdown complete")
