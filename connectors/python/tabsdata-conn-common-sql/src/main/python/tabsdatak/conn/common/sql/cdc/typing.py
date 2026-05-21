"""Public typing primitives for SQL-CDC connectors.

These are framework-validated dataclasses (``@_api.dataclass``) so
they participate in the same serialization / construction error
machinery as the rest of the connector API surface. Per-database
connectors expose them as field types on their ``StreamSrc``
subclasses.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, TypeAlias, Union

import tabsdatak._api as _api
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode


# ---------------------------------------------------------------------
# Table specs
# ---------------------------------------------------------------------

# A single fully-qualified table reference. Validation of the
# `schema.table` shape is delegated to the per-database dialect at
# ingester startup -- this typealias is the framework-visible string.
QualifiedTableSpec: TypeAlias = str

# Source connectors normally take a list of tables to track.
# ``Union[str, list[str]]`` mirrors the legacy CDC trigger shape so
# a single-table source can write the bare string directly.
QualifiedTablesSpec: TypeAlias = Union[QualifiedTableSpec, list[QualifiedTableSpec]]


# ---------------------------------------------------------------------
# Start-position primitives
# ---------------------------------------------------------------------

# Relative position keywords accepted by every dialect. ``"head"``
# rewinds to the oldest available change in the source's retention
# window; ``"tail"`` skips to the live edge and only emits changes
# that occur after the trigger starts.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]


@_api.dataclass(SqlCdcErrorCode.SQL_CDC_8, frozen=True, kw_only=True)
class TimestampPosition:
    """Start CDC ingestion at the change-stream offset that
    corresponds to a wall-clock timestamp. The per-database ingester
    is responsible for translating ``ts`` into its native offset
    shape (GTID set, LSN, log+position, ...).
    """

    ts: datetime


# ---------------------------------------------------------------------
# Output format
# ---------------------------------------------------------------------

# Layout for the source columns payload on each emitted change row:
#   * ``"columns"`` -- one parquet column per source column, prefixed
#     by `CDC_DATA_COL_NEW_PREFIX` / `CDC_DATA_COL_OLD_PREFIX`.
#   * ``"struct"``  -- a single struct column carrying every source
#     column as a nested field.
#   * ``"map"``     -- a single utf8 column holding a JSON-encoded
#     map of source columns to values.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@_api.dataclass(SqlCdcErrorCode.SQL_CDC_7, frozen=True, kw_only=True)
class CdcFormat:
    """Output format for emitted CDC rows.

    :param columns_format: Layout for the new (and old, when present)
        columns payload. See ``CdcColumnsFormatSpec`` for the options.
    :param flatten_columns: When ``True`` the new columns are inlined
        as top-level columns -- the connector's downstream consumer
        sees an ordinary tabular row and ``columns_format`` only
        affects the *old*-columns layout.
    """

    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
