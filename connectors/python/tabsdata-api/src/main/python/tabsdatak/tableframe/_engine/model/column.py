#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from tabsdatak.tableframe._engine.taxonomy.column import (
    Inception,
    Language,
    SystemColumnCategory,
)

if TYPE_CHECKING:
    import polars as pl_
    from polars.datatypes import DataTypeClass as DataTypeClass_

    SystemColumnDataType = pl_.DataType | DataTypeClass_
    SystemColumnDefault = pl_.Expr | str | datetime | None


@dataclass(slots=True, frozen=True, eq=True)
class SystemColumn:
    name: str
    category: SystemColumnCategory
    dtype: Callable[[], SystemColumnDataType]
    default: Callable[[], SystemColumnDefault]
    language: Language
    inception: Inception
    required: bool
    pristine: bool
    generator: Any
    aggregation: Any
