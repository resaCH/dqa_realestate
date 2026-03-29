"""
src/utils/logger.py
--------------------
Zentrales Logging via loguru.
"""

import sys

from loguru import logger

from src.utils.config import settings


def setup_logger() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — {message}"
        ),
        colorize=True,
    )
    logger.add(
        "logs/dqa_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="DEBUG",
        encoding="utf-8",
    )
