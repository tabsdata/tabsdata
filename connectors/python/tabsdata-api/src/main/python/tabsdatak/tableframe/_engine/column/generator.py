#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_TEMP_COLUMN_PREFIX,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.column import (
        ColumnDefault as ColumnDefault_,
    )
    from tabsdatak.tableframe._engine.model.frame import (
        DataVersion as DataVersion_,
    )
    from tabsdatak.tableframe.datatypes import ColumnDataType as ColumnDataType_


@dataclass(frozen=True, slots=True)
class GeneratorContext:
    index: int | None
    version: DataVersion_ | None
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


class IdxGenerator:
    def __init__(self) -> None:
        self._index = 0

    @property
    def index(self) -> int:
        return self._index

    @index.setter
    def index(self, value: int) -> None:
        self._index = value

    def __call__(self) -> int:
        idx = self._index
        self.index += 1
        return idx


def as_expression(
    value: ColumnDefault_,
    dtype: ColumnDataType_,
) -> pl_.Expr:
    import polars as pl

    if isinstance(value, pl.Expr):
        return value
    return pl.lit(value, dtype=dtype)


def seed_default(
    lf: pl_.LazyFrame,
    column: str,
    dtype: ColumnDataType_,
    default: ColumnDefault_,
    context: GeneratorContext,
) -> pl_.LazyFrame:
    # A 0-column, 0-row frame would gain a spurious single row when a literal is
    # assigned to a new column; build the column through a typed empty frame instead.
    if context.is_void:
        import polars as pl

        return pl.DataFrame(schema=[(column, dtype)]).lazy()
    return lf.with_columns(as_expression(default, dtype).alias(column))
