#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path


def beat(path: Path) -> None:
    path.touch()


class LivenessLogHandler(logging.Handler):
    path: Path

    def __init__(self, path: Path) -> None:
        super().__init__(level=logging.NOTSET)
        self.path = path
        try:
            beat(self.path)
        except Exception:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            beat(self.path)
        except Exception:
            self.handleError(record)


def install_liveness_handler(path: Path) -> LivenessLogHandler:
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, LivenessLogHandler):
            root.removeHandler(handler)
    handler = LivenessLogHandler(path)
    root.addHandler(handler)
    if root.level == logging.NOTSET or root.level > logging.DEBUG:
        root.setLevel(logging.DEBUG)
    return handler
