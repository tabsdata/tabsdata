#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias, Union

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

QualifiedTableSpec: TypeAlias = str

QualifiedTablesSpec: TypeAlias = Union[QualifiedTableSpec, list[QualifiedTableSpec]]

RelativePositionSpec: TypeAlias = Literal["head", "tail"]


@dataclass(frozen=True, init=False)
class TimestampPositionSpec:
    ts: datetime

    @_td_validate_call(ErrorCode.CDC13)
    def __init__(self, ts: datetime):
        object.__setattr__(self, "ts", ts)


CdcValuesFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@dataclass(frozen=True, init=False)
class CdcFormatSpec:
    values_format: CdcValuesFormatSpec = "columns"
    flat_new_values: bool = True

    @_td_validate_call(ErrorCode.CDC14)
    def __init__(
        self,
        values_format: CdcValuesFormatSpec = "columns",
        flat_new_values: bool = True,
    ):
        object.__setattr__(self, "values_format", values_format)
        object.__setattr__(self, "flat_new_values", flat_new_values)
