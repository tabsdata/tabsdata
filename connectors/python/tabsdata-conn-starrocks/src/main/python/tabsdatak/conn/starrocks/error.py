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


class StarRocksErrorCode(ErrorCode):
    STARROCKS_1 = ErrorDef(
        ConnInitException,
        "StarRocksConn validation failed",
    )  # StarRocksConn
    STARROCKS_2 = ErrorDef(
        ConnInitException,
        "StarRocksDest validation failed",
    )  # StarRocksDest
