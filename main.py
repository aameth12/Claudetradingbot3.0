"""Entry point for the AI Multi-Agent Day Trading Bot."""

import asyncio
import atexit
import os
import sys

import yaml
from dotenv import load_dotenv
from loguru import logger

from utils.logger import setup_logger

_LOCK_FILE = "bot.lock"


def _acquire_lock():
    """Prevent multiple simultaneous bot instances (avoids Telegram Conflict errors)."""
    if os.path.exists(_LOCK_FILE):
        try:
            with open(_LOCK_FILE) as f:
                pid = int(f.read().strip())
            # Check if the PID is still alive
            os.kill(pid, 0)
            logger.error(
                f"Another bot instance is already running (PID {pid}). "
                "Stop it first or delete bot.lock if it crashed."
            )
            sys.exit(1)
        except (ValueError, OSError):
            # Process no longer exists — stale lock, overwrite it
            pass

    with open(_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

    atexit.register(lambda: os.unlink(_LOCK_FILE) if os.path.exists(_LOCK_FILE) else None)


def load_config(path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.warning(f"Config file not found: {path}, using defaults")
        return {}


def main():
    # Prevent multiple instances (avoids Telegram Conflict errors)
    _acquire_lock()

    # Windows asyncio policy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load environment variables
    load_dotenv()

    # Load config
    config = load_config()

    # Setup logging
    log_config = config.get("logging", {})
    setup_logger(
        level=log_config.get("level", "INFO"),
        log_file=log_config.get("file", "logs/trading_bot.log"),
        rotation=log_config.get("rotation", "100 MB"),
    )

    logger.info("=" * 60)
    logger.info("AI Multi-Agent Day Trading Bot v2.0")
    logger.info("=" * 60)

    from ib_insync import util
    from orchestrator.orchestrator import Orchestrator

    # MUST be called before util.run() — patches asyncio to allow nested
    # event loops so ib_insync 0.9.86's synchronous methods (which internally
    # call loop.run_until_complete via util.syncAwait) work inside a running
    # async context without raising "This event loop is already running".
    # nest_asyncio is already in requirements.txt.
    util.patchAsyncio()

    orchestrator = Orchestrator(config)

    try:
        util.run(orchestrator.start())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        logger.info("Bot terminated")


if __name__ == "__main__":
    main()
