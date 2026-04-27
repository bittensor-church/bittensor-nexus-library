from __future__ import annotations

import logging
import sys
from typing import TextIO

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a logger scoped to the provided name."""
    return logging.getLogger(name)


def configure_default_logging(
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
    *,
    force: bool = False,
) -> None:
    """Configure a sane default handler for quickstarts without affecting host apps."""
    if stream is None:
        stream = sys.stdout
    logging.basicConfig(level=level, format=_LOG_FORMAT, stream=stream, force=force)


def _ensure_null_handler() -> None:
    """Attach a NullHandler so library imports never emit "No handler" warnings."""
    package_logger = logging.getLogger("nexus")
    if not any(isinstance(handler, logging.NullHandler) for handler in package_logger.handlers):
        package_logger.addHandler(logging.NullHandler())


_ensure_null_handler()

__all__ = ["configure_default_logging", "get_logger"]
