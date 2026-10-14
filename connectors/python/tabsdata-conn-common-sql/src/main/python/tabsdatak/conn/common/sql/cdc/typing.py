#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias, Union

from tabsdatak import _api
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode

QualifiedTableSpec: TypeAlias = str
"""A single qualified `schema.table` name of a table to track."""

QualifiedTablesSpec: TypeAlias = Union[QualifiedTableSpec, list[QualifiedTableSpec]]
"""One qualified `schema.table` name, or a list of them."""

RelativePositionSpec: TypeAlias = Literal["head", "tail"]
"""Relative CDC start position: `"head"` replays from the oldest
change still retained, `"tail"` starts at the current position so only
changes made after the stream starts are emitted."""


FULL_START = "full"
"""The `start_from` value asking for a snapshot of every tracked table before
the changes that follow it."""


@dataclass(frozen=True, init=False)
class TimestampPosition:
    """A CDC start position given as a wall-clock timestamp.

    :categories: source

    Args:
        ts: The timestamp at or after which to start reading changes.
    """

    ts: datetime

    @_api.validate_call(SqlCdcErrorCode.CDC_13)
    def __init__(self, ts: datetime):
        object.__setattr__(self, "ts", ts)


CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]
"""Layout of the change-values payload on each emitted CDC row: one of
`"columns"` (each value as its own column), `"struct"` (a single
nested struct) or `"map"` (a single name-to-value map)."""


@dataclass(frozen=True, init=False)
class CdcFormat:
    """Output shape for the change data a CDC source emits.

    :categories: source

    Args:
        columns_format: Layout for the change values -- one of
            `"columns"`, `"struct"` or `"map"`. Defaults to
            `"columns"`.
        flatten_columns: When `True` (default), the new values are
            inlined as top-level columns.
    """

    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True

    @_api.validate_call(SqlCdcErrorCode.CDC_14)
    def __init__(
        self,
        columns_format: CdcColumnsFormatSpec = "columns",
        flatten_columns: bool = True,
    ):
        object.__setattr__(self, "columns_format", columns_format)
        object.__setattr__(self, "flatten_columns", flatten_columns)
