#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest


def logger_levels() -> dict[str, int]:
    return {
        name: logger.level
        for name, logger in list(logging.Logger.manager.loggerDict.items())
        if isinstance(logger, logging.Logger)
    }


# Connectors apply their `logging_level` option to their loggers and never undo
# it, so without this a test inherits the levels earlier tests in its worker left.
@pytest.fixture(autouse=True)
def _restore_logger_levels() -> Iterator[None]:
    before = logger_levels()
    yield
    for name, level in logger_levels().items():
        kept = before.get(name, logging.NOTSET)
        if level != kept:
            logging.getLogger(name).setLevel(kept)
