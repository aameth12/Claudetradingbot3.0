"""Orchestrator — Message bus, heartbeat, position poller."""

import asyncio
import signal
import sys

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
        # Merge allow_shorts/allow_longs from strategy section into risk config
        # (they live under strategy: in config.yaml but RiskAgent enforces them)
        strategy_cfg = config.get("strategy", {})
        risk_cfg = {
            **config.get("risk", {}),
            "allow_shorts": strategy_cfg.get("allow_shorts", True),
            "allow_longs": strategy_cfg.get("allow_longs", True),
        }
        self.risk_agent = RiskAgent(risk_cfg, orchestrator=self)
        self.execution_agent = ExecutionAgent(config.get("risk", {}), orchestrator=self)
        self.telegram_agent = TelegramAgent(config.get("telegram", {}), orchestrator=self)
        self.performance_agent = PerformanceAgent(config.get("performance", {}), orchestrator=self)

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

        # Give IBKR time to finish connecting
        await asyncio.sleep(5)

        # Start background tasks
        self._tasks.append(asyncio.create_task(self._heartbeat(), name="heartbeat"))
        self._tasks.append(asyncio.create_task(self._position_poller(), name="position_poller"))

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
        """30-second loop: push live position P&L and risk data to TelegramAgent."""
        while self._running:
            await asyncio.sleep(30)
            try:
                # Get market data snapshots for open positions
                market_data = {}
                for symbol in list(self.execution_agent._open_trades.keys()):
                    snapshot = self.ibkr_agent.get_snapshot(symbol)
                    live_price = None
                    if snapshot:
                        live_price = snapshot.get("last") or snapshot.get("bid") or snapshot.get("close")
                        market_data[symbol] = snapshot

                    # yfinance fallback when IBKR ticker has no live price
                    if not live_price:
                        try:
                            import yfinance as yf
                            price = yf.Ticker(symbol).fast_info.last_price
                            if price and price == price:  # not NaN
                                market_data[symbol] = {
                                    "last": float(price), "bid": None,
                                    "ask": None, "close": float(price),
                                }
                        except Exception:
                            pass

                # Reconcile IBKR positions with bot-tracked trades
                # If IB has a position the bot doesn't know about, restore it
                try:
                    ibkr_positions = self.ibkr_agent.ib.positions()
                    tracked = set(self.execution_agent._open_trades.keys())
                    untracked = [
                        {"symbol": p.contract.symbol, "quantity": int(p.position), "avg_cost": p.avgCost}
                        for p in ibkr_positions
                        if p.position != 0 and p.contract.symbol not in tracked
                    ]
                    if untracked:
                        logger.info(f"Reconciler: found {len(untracked)} untracked IBKR position(s): {[u['symbol'] for u in untracked]}")
                        await self.execution_agent.inbox.put(Message(
                            sender="Orchestrator",
                            recipient="ExecutionAgent",
                            type="ibkr_reconnected",
                            payload={"open_positions": untracked},
                        ))
                except Exception as rec_e:
                    logger.debug(f"Position reconcile error: {rec_e}")

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

            except Exception as e:
                logger.warning(f"Position poller error: {e}")

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
