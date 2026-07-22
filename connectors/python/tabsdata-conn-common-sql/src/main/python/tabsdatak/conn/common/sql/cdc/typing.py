#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias, Union

from tabsdatak.conn.common.sql.cdc.error import (
    SqlCdcErrorCode,
    cdc_validate_call,
)

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

    @cdc_validate_call(SqlCdcErrorCode.CDC_13)
    def __init__(self, ts: datetime):
        object.__setattr__(self, "ts", ts)


CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@dataclass(frozen=True, init=False)
class CdcFormat:
    """
    Specifies the output format for CDC change data.

    Attributes:
        columns_format: The layout for change values ("columns", "struct", or "map").
        flatten_columns: Whether to flatten new values into top-level columns.
    """

    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True

    @cdc_validate_call(SqlCdcErrorCode.CDC_14)
    def __init__(
        self,
        columns_format: CdcColumnsFormatSpec = "columns",
        flatten_columns: bool = True,
    ):
        object.__setattr__(self, "columns_format", columns_format)
        object.__setattr__(self, "flatten_columns", flatten_columns)
