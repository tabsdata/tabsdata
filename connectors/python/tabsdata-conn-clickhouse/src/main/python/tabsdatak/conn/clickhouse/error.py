from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class ClickHouseValidateException(ConnException):
    """Cross-field validation failure for the ClickHouse connector."""


class ClickHouseRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the ClickHouse connector."""


class ClickHouseErrorCode(ErrorCode):
    CLICKHOUSE_1 = ErrorDef(
        ConnInitException,
        "ClickHouseConn validation failed",
    )  # ClickHouseConn
    CLICKHOUSE_2 = ErrorDef(
        ConnInitException,
        "ClickHouseDest validation failed",
    )  # ClickHouseDest
    CLICKHOUSE_3 = ErrorDef(
        ClickHouseValidateException,
        "ClickHouseDest.tables must be non-empty",
    )  # ClickHouseDest.validate()
    CLICKHOUSE_4 = ErrorDef(
        ClickHouseRuntimeException,
        "ClickHouseDest tables/slot count mismatch (tables={tables},"
        " slots={slots})",
    )  # _ClickHouseDestPlugin: defensive recheck
    CLICKHOUSE_5 = ErrorDef(
        ClickHouseRuntimeException,
        "ClickHouse destination connection failed (host={host})",
    )  # _ClickHouseDestPlugin.write_out: client connect failure
    CLICKHOUSE_6 = ErrorDef(
        ClickHouseRuntimeException,
        "ClickHouse bulk load failed (table={table})",
    )  # _ClickHouseDestPlugin.write_out: load failure
    CLICKHOUSE_7 = ErrorDef(
        ClickHouseRuntimeException,
        "Unsupported ClickHouse staging type: {staging}",
    )  # _ClickHouseDestPlugin.write_out: bad staging type
    CLICKHOUSE_8 = ErrorDef(
        ConnInitException,
        "Cannot create S3Location",
    )  # S3Location (clickhouse-local)
    CLICKHOUSE_9 = ErrorDef(
        ConnInitException,
        "Cannot create AzBlobLocation",
    )  # AzBlobLocation (clickhouse-local)
    CLICKHOUSE_10 = ErrorDef(
        ConnInitException,
        "Cannot create GCSLocation",
    )  # GCSLocation (clickhouse-local, GCPHmac credentials)
