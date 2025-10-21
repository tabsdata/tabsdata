#
# Copyright 2025 Tabs Data Inc.
#

import logging
import platform
import sys

from rich.console import Console
from rich.logging import RichHandler


class NewlineOnFirstLog(logging.Filter):
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
