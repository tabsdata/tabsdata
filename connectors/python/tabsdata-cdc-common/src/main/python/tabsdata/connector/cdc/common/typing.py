#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias, Union

QualifiedTableSpec: TypeAlias = str

QualifiedTablesSpec: TypeAlias = Union[QualifiedTableSpec, list[QualifiedTableSpec]]

OriginPositionSpec: TypeAlias = Literal["origin"]


@dataclass(frozen=True)
class TimestampPositionSpec:
    ts: datetime


CdcValuesFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@dataclass(frozen=True)
class CdcFormatSpec:
    values_format: CdcValuesFormatSpec = "columns"
    flat_new_values: bool = True
