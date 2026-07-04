#
# Copyright 2026 Tabs Data Inc.
#

import logging
from typing import Literal

from rich.logging import RichHandler

LOG_FORMAT = "{message}"
LOG_STYLE: Literal["{"] = "{"

LOG_LEVEL_COLORS = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold red",
}


class LevelColoredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = LOG_LEVEL_COLORS.get(record.levelname)
        return f"[{color}]{message}[/{color}]" if color else message


def configure() -> None:
    handler = RichHandler(
        show_time=False,
        show_level=False,
        show_path=False,
        rich_tracebacks=True,
        markup=True,
    )
    handler.setFormatter(LevelColoredFormatter(LOG_FORMAT, style=LOG_STYLE))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
