"""Shared building blocks for SQL-based CDC stream sources.

This sub-package hosts the reusable layer used by per-database CDC
connectors (MySQL, Postgres, Db2). Per-database packages bring
their dialect-specific ingester (binlog / WAL / log capture) and
plug it into the common runner / sentinels / model defined here.

Public surface (re-exported below): typing primitives that callers
declare on their ``StreamSrc`` subclasses (``CdcFormat``,
``TimestampPosition``, ``RelativePositionSpec``,
``QualifiedTablesSpec``). Everything else is internal machinery.
"""

from __future__ import annotations

from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat,
    CdcColumnsFormatSpec,
    QualifiedTableSpec,
    QualifiedTablesSpec,
    RelativePositionSpec,
    TimestampPosition,
)

__all__ = [
    "CdcFormat",
    "CdcColumnsFormatSpec",
    "QualifiedTableSpec",
    "QualifiedTablesSpec",
    "RelativePositionSpec",
    "TimestampPosition",
]
