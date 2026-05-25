#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import logging.config
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

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
