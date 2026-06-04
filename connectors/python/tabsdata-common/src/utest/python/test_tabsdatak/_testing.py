#
# Copyright 2026 Tabs Data Inc.
#

"""Shared test-side helpers for tabsdatak.

Lives in the ``test_tabsdatak`` namespace so connector test trees
can import these helpers without dragging test-only dependencies
(e.g. ``rich``) into the production ``tabsdatak._logging`` module.

Currently exposes:

* :func:`setup_tests_logging` -- configure the root logger for a
  pytest run (RichHandler to stderr, ERROR level, ``filelock``
  bumped to INFO).
* :class:`NewlineOnFirstLog` -- log filter that injects a leading
  newline on the first emitted record (Unix only; on Windows it
  drops everything below CRITICAL to keep the console clean).

Consumers (pytest conftests) should ``from test_tabsdatak._testing
import setup_tests_logging``.
"""

from __future__ import annotations

import logging
import platform
import sys

from rich.console import Console
from rich.logging import RichHandler


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
