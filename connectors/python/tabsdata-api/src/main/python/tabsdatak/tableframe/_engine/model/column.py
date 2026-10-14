#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from tabsdatak.tableframe._engine.taxonomy.column import (
    Inception,
    Language,
    SystemColumnCategory,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe.datatypes import ColumnDataType as ColumnDataType_

    ColumnDefault = pl_.Expr | str | int | datetime | list | None


@dataclass(slots=True, frozen=True, eq=True)
class SystemColumn:
    name: str
    category: SystemColumnCategory
    dtype: ColumnDataType_
    default: ColumnDefault
    language: Language
    inception: Inception
    required: bool
    pristine: bool
    generator: Any
    aggregation: Any
