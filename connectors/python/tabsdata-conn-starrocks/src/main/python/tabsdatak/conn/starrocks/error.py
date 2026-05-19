from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class StarRocksValidateException(ConnException):
    """Cross-field validation failure for the StarRocks connector."""


class StarRocksRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the StarRocks connector."""


class StarRocksErrorCode(ErrorCode):
    STARROCKS_1 = ErrorDef(
        ConnInitException,
        "StarRocksConn validation failed",
    )  # StarRocksConn
    STARROCKS_2 = ErrorDef(
        ConnInitException,
        "StarRocksDest validation failed",
    )  # StarRocksDest
    STARROCKS_3 = ErrorDef(
        StarRocksRuntimeException,
        "StarRocksDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _StarRocksDestPlugin: defensive recheck
    STARROCKS_4 = ErrorDef(
        StarRocksRuntimeException,
        "StarRocks destination connection failed (host={host}, port={port})",
    )  # _StarRocksDestPlugin.write_out: engine / connect failure
    STARROCKS_5 = ErrorDef(
        StarRocksRuntimeException,
        "StarRocks write failed (table={table})",
    )  # _StarRocksDestPlugin: write failure inside write_out
    STARROCKS_6 = ErrorDef(
        StarRocksRuntimeException,
        "StarRocks staging upload failed (table={table})",
    )  # _StarRocksDestPlugin: failure uploading parquet to staging
    STARROCKS_7 = ErrorDef(
        StarRocksRuntimeException,
        "StarRocks staging location type is not supported: {kind}",
    )  # build_files_function: unknown CloudLocation subtype
