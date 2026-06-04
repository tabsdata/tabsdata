#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import logging.config
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.logging import RichHandler

TRACE: int = 5

logging.addLevelName(TRACE, "TRACE")


def _trace(self: logging.Logger, msg: object, *args: object, **kwargs: Any) -> None:
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


logging.Logger.trace = _trace  # type: ignore[attr-defined]


class CriFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
        stream = "stderr" if record.levelno >= logging.WARNING else "stdout"
        message = super().format(record)
        return f"{timestamp} {stream} F {message}"


class StdoutLevelFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING


class StderrLevelFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "_logging.yaml"


def setup_logging(config_path: str | Path | None = None) -> None:
    path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_PATH
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    logging.config.dictConfig(config)


# --- Test logging ----------------------------------------------------
#
# Pretty-printed test-side logging via rich. Used by pytest conftests
# in connector packages. Production code uses ``setup_logging`` above.


class NewlineOnFirstLog(logging.Filter):
    """Inject a leading newline on the first emitted record, and on
    Windows drop everything else (rich's handler upsets the Windows
    console)."""

    def __init__(self):
        super().__init__()
        self.first = True

    def filter(self, record):
        if self.first:
            self.first = False
            record.msg = "\n" + str(record.msg)
        if platform.system() == "Windows":
            return False
        else:
            return record.levelno >= logging.CRITICAL


def setup_tests_logging() -> logging.Logger:
    """Configure the root logger for pytest runs: ERROR-level, a
    single :class:`RichHandler` writing to stderr, ``filelock`` bumped
    to INFO."""
    logger = logging.getLogger(None)
    logger.setLevel(logging.ERROR)
    logger.propagate = True
    logger.handlers.clear()
    handler = RichHandler(
        rich_tracebacks=True,
        show_time=False,
        show_level=True,
        show_path=False,
        console=Console(file=sys.stderr),
    )
    handler.addFilter(NewlineOnFirstLog())
    logger.addHandler(handler)
    logging.getLogger("filelock").setLevel(logging.INFO)

    return logger
