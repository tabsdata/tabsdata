#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations


class StreamRestart(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
