from __future__ import annotations

import logging
import sys
from typing import TextIO

from litestar.logging.config import LoggingConfig

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a logger scoped to the provided name."""
    return logging.getLogger(name)


def host_friendly_logging_config() -> LoggingConfig:
    """
    Return a Litestar logging config that leaves the host application's root logger untouched.

    Litestar reconfigures the root logger whenever an app is built with its default ``LoggingConfig``,
    which clobbers whatever logging the embedding application has already installed (for example a
    structlog JSON renderer). Disabling ``configure_root_logger`` keeps each Nexus-owned Litestar app
    scoped to its own ``litestar`` logger and leaves the root logger — and therefore the host's chosen
    log format — alone.
    """
    return LoggingConfig(configure_root_logger=False)


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

__all__ = ["configure_default_logging", "get_logger", "host_friendly_logging_config"]
