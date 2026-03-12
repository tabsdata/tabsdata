#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum, auto


class BulkLoadStatus(Enum):
    COMPLETED = auto()
    FAILED = auto()
    PENDING = auto()
