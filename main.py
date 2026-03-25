"""Entry point for the AI Multi-Agent Day Trading Bot."""

import asyncio
import sys

import yaml
from dotenv import load_dotenv
from loguru import logger

from orchestrator.orchestrator import Orchestrator
from utils.logger import setup_logger


def load_config(path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.warning(f"Config file not found: {path}, using defaults")
        return {}


def main():
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

    # Create and run orchestrator
    orchestrator = Orchestrator(config)

    try:
        asyncio.run(orchestrator.start())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        logger.info("Bot terminated")


if __name__ == "__main__":
    main()
