#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def present[T](
    tables: Sequence[T | None],
    *aligned: Sequence[Any],
) -> tuple[list[T], ...]:
    positions = [position for position, table in enumerate(tables) if table is not None]
    for values in aligned:
        if len(values) != len(tables):
            raise ValueError(
                f"{len(values)} values are aligned with {len(tables)} tables",
            )
    return (
        [tables[position] for position in positions],
        *([values[position] for position in positions] for values in aligned),
    )
