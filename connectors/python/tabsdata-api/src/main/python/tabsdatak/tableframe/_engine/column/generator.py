#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Protocol

from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_TEMP_COLUMN_PREFIX,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.column import (
        SystemColumnDataType as SystemColumnDataType_,
        SystemColumnDefault as SystemColumnDefault_,
    )
    from tabsdatak.tableframe._engine.model.tableframe import (
        DataVersionProperties as DataVersionProperties_,
    )


@dataclass(frozen=True, slots=True)
class GeneratorContext:
    index: int | None
    properties: DataVersionProperties_ | None
    row_index_column: str | None
    is_void: bool


class Generator(Protocol):
    def apply(
        self,
        lf: pl_.LazyFrame,
        column: str,
        context: GeneratorContext,
    ) -> pl_.LazyFrame: ...


def temp_column() -> str:
    return f"{TD_TEMP_COLUMN_PREFIX}{uuid.uuid4().hex}"


def seed_default(
    lf: pl_.LazyFrame,
    column: str,
    dtype: Callable[[], SystemColumnDataType_],
    default: Callable[[], SystemColumnDefault_],
    context: GeneratorContext,
) -> pl_.LazyFrame:
    # A 0-column, 0-row frame would gain a spurious single row when a literal is
    # assigned to a new column; build the column through a typed empty frame instead.
    if context.is_void:
        import polars as pl

        return pl.DataFrame(schema=[(column, dtype())]).lazy()
    return lf.with_columns(default().alias(column))
