#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def column_range(footers: Iterable[Any], column: str) -> tuple[Any, Any] | None:
    """The smallest and largest value of ``column`` across every row group the
    Parquet footers describe, read off their statistics without touching the
    data; ``(None, None)`` when every value is null, and ``None`` when a row
    group carries no statistics for the column, so its values cannot be
    bounded."""
    footers = list(footers or ())
    if not footers:
        return None
    low = high = None
    for meta in footers:
        for index in range(meta.num_row_groups):
            group = meta.row_group(index)
            chunks = [
                group.column(i)
                for i in range(group.num_columns)
                if group.column(i).path_in_schema == column
            ]
            if not chunks:
                return None
            stats = chunks[0].statistics
            if stats is None:
                return None
            if not stats.has_min_max:
                if stats.has_null_count and stats.null_count == group.num_rows:
                    continue
                return None
            low = stats.min if low is None else min(low, stats.min)
            high = stats.max if high is None else max(high, stats.max)
    return low, high
