#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum


class RowOperation(Enum):
    """Values are persisted in stored data; do not change them."""

    ROW = 0
    UNDEFINED = 1
    GROUP_MIN = 2
    GROUP_MAX = 3
    GROUP_SUM = 4
    GROUP_LEN = 5
    GROUP_COUNT = 6
    GROUP_MEAN = 7
    GROUP_MEDIAN = 8
    GROUP_UNIQUE = 9
