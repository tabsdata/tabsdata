#
# Copyright 2026 Tabsdata Inc.
#

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
    """Cross-field validation failure for the ClickHouse connector.

    :categories: error
    """


class ClickHouseRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the ClickHouse connector.

    :categories: error
    """


class ClickHouseErrorCode(ErrorCode):
    """Error codes for the ClickHouse connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `ClickHouseErrorCode.CLICKHOUSE_N.exception`.
    """

    CLICKHOUSE_1 = ErrorDef(
        ConnInitException,
        "ClickHouseDestConn validation failed",
    )  # ClickHouseDestConn
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
        "ClickHouseDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _ClickHouseDestPlugin: defensive recheck
    CLICKHOUSE_5 = ErrorDef(
        ClickHouseRuntimeException,
        "ClickHouse destination connection failed (host={host})",
    )  # ClickHouseDialect / _ClickHouseDestPlugin.write_out: connection failure
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

    # --- Field-value rules (tabsdatak.conn.clickhouse) --------------
    CLICKHOUSE_11 = ErrorDef(
        ConnInitException,
        "not a valid ClickHouse database name (letters, digits and underscores, "
        "not starting with a digit): {value!r}",
    )  # DatabaseSpec
    CLICKHOUSE_12 = ErrorDef(
        ClickHouseValidateException,
        "GCSLocation.cfg cannot set {key!r}: ClickHouse reads GCS staging"
        " through Google's own S3-compatible endpoint, so a custom endpoint"
        " would write the parquet where ClickHouse cannot read it",
    )  # GCSLocation.validate()
    CLICKHOUSE_13 = ErrorDef(
        ClickHouseRuntimeException,
        "if_table_exists='replace' needs a database with the Atomic, Shared or"
        " Replicated engine, to exchange tables; database {database!r} uses"
        " {engine!r}",
    )  # ClickHouseDialect.check_swappable
