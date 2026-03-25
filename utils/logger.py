"""Loguru-based logging setup for the trading bot."""

import sys
from pathlib import Path

from loguru import logger


def setup_logger(level: str = "INFO", log_file: str = "logs/trading_bot.log", rotation: str = "100 MB"):
    """Configure loguru logger with console and file sinks."""
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    logger.add(sys.stderr, format=log_format, level=level, colorize=True)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_path),
        format=log_format,
        level=level,
        rotation=rotation,
        retention="7 days",
        compression="zip",
        enqueue=True,
    )

    return logger
