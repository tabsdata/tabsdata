#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from collections.abc import Mapping


def size_of_values(values: Mapping | None) -> int:
    if values is None:
        return 0
    held = sum(sys.getsizeof(value) for value in values.values())
    return sys.getsizeof(values) + held
