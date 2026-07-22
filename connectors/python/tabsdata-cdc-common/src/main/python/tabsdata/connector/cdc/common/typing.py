#
# Copyright 2026 Tabsdata Inc.
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
class TimestampPosition:
    """
    Specifies a CDC start position based on a timestamp.

    Attributes:
        ts: The timestamp from which to start reading changes.
    """

    ts: datetime

    @_td_validate_call(ErrorCode.CDC13)
    def __init__(self, ts: datetime):
        object.__setattr__(self, "ts", ts)


CdcValuesFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@dataclass(frozen=True, init=False)
class CdcFormat:
    """
    Specifies the output format for CDC change data.

    Attributes:
        values_format: The layout for change values ("columns", "struct", or "map").
        flatten_values: Whether to flatten new values into top-level columns.
    """

    values_format: CdcValuesFormatSpec = "columns"
    flatten_values: bool = True

    @_td_validate_call(ErrorCode.CDC14)
    def __init__(
        self,
        values_format: CdcValuesFormatSpec = "columns",
        flatten_values: bool = True,
    ):
        object.__setattr__(self, "values_format", values_format)
        object.__setattr__(self, "flatten_values", flatten_values)
