#
# Copyright 2026 Tabs Data Inc.
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
    from polars.datatypes import DataTypeClass as DataTypeClass_

    from tabsdatak.tableframe import datatypes as datatypes_

    SystemColumnDataType = datatypes_.DataType | DataTypeClass_
    SystemColumnDefault = pl_.Expr | str | int | datetime | list | None


@dataclass(slots=True, frozen=True, eq=True)
class SystemColumn:
    name: str
    category: SystemColumnCategory
    dtype: SystemColumnDataType
    default: SystemColumnDefault
    language: Language
    inception: Inception
    required: bool
    pristine: bool
    generator: Any
    aggregation: Any
