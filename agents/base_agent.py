"""Abstract base agent and Message dataclass for the multi-agent trading bot."""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from loguru import logger


@dataclass
class Message:
    sender: str
    recipient: str  # agent name or "broadcast"
    type: str
    payload: dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class BaseAgent(ABC):
    """Abstract base class for all trading bot agents."""

    def __init__(self, name: str, orchestrator=None):
        self.name = name
        self.orchestrator = orchestrator
        self.inbox: asyncio.Queue[Message] = asyncio.Queue()
        self._running = False
        logger.info(f"Agent created: {self.name}")

    async def start(self):
        """Start the agent's main loop."""
        self._running = True
        logger.info(f"{self.name} started")
        try:
            await self.run()
        except asyncio.CancelledError:
            logger.info(f"{self.name} cancelled")
        except Exception as e:
            logger.exception(f"{self.name} crashed: {e}")
        finally:
            self._running = False

    async def stop(self):
        """Stop the agent."""
        self._running = False
        logger.info(f"{self.name} stopped")

    @abstractmethod
    async def run(self):
        """Main loop — must be implemented by each agent."""
        ...

    @abstractmethod
    async def handle_message(self, message: Message):
        """Handle an incoming message — must be implemented by each agent."""
        ...

    async def _process_inbox(self):
        """Process all messages currently in the inbox."""
        while not self.inbox.empty():
            try:
                message = self.inbox.get_nowait()
                await self.handle_message(message)
            except Exception as e:
                logger.exception(f"{self.name} error handling message: {e}")

    def send(self, recipient: str, msg_type: str, payload: dict | None = None):
        """Send a message to another agent via the orchestrator."""
        message = Message(
            sender=self.name,
            recipient=recipient,
            type=msg_type,
            payload=payload or {},
        )
        if self.orchestrator:
            asyncio.create_task(self.orchestrator.dispatch(message))
        else:
            logger.warning(f"{self.name}: no orchestrator, message dropped: {msg_type}")

    def broadcast(self, msg_type: str, payload: dict | None = None):
        """Broadcast a message to all agents via the orchestrator."""
        message = Message(
            sender=self.name,
            recipient="broadcast",
            type=msg_type,
            payload=payload or {},
        )
        if self.orchestrator:
            asyncio.create_task(self.orchestrator.dispatch(message))
        else:
            logger.warning(f"{self.name}: no orchestrator, broadcast dropped: {msg_type}")
