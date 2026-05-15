#
# Copyright 2026 Tabs Data Inc.
#

"""Logging setup for tdrunner commands.

Per `docs/01-CommandBasics.md`: INFO/DEBUG/TRACE go to stdout, WARN/ERROR
go to stderr.
"""

from __future__ import annotations

import logging
import sys

TRACE: int = 5

# Idempotent: addLevelName overwrites silently when called twice.
logging.addLevelName(TRACE, "TRACE")


def _trace(self, msg, *args, **kwargs):
    """Log ``msg % args`` at TRACE level. Mirrors :meth:`Logger.debug` /
    :meth:`Logger.info` so callers can write ``logger.trace(...)``
    instead of the asymmetric ``logger.log(TRACE, ...)``.
    """
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


# Expose `logger.trace(...)` on every Logger in the process so module-level
# loggers (created at import time, before any setup_logging call) also pick
# it up.
logging.Logger.trace = _trace


class _StdoutFilter(logging.Filter):
    """Filter that lets through records strictly below WARNING."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure a logger so INFO/DEBUG/TRACE go to stdout and WARN/ERROR
    go to stderr.

    Returns the configured logger. Replaces existing handlers on the same
    name so this function is safe to call repeatedly.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(TRACE)
    stdout_handler.addFilter(_StdoutFilter())
    logger.addHandler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    logger.addHandler(stderr_handler)

    logger.propagate = False
    return logger
