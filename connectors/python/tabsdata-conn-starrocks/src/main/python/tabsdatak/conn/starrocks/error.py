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
    STARROCKS_3 = ErrorDef(
        ConnInitException,
        "StarRocks identifier must be non-empty",
    )  # _validate_identifier: empty
    STARROCKS_4 = ErrorDef(
        ConnInitException,
        "StarRocks URI must start with starrocks:// or mysql:// (optionally"
        " driver-qualified): {value!r}",
    )  # _validate_uri
    STARROCKS_5 = ErrorDef(
        ConnInitException,
        "StarRocks identifier exceeds {max} chars: {value!r}",
    )  # _validate_identifier: too long
    STARROCKS_6 = ErrorDef(
        ConnInitException,
        "backtick-quoted StarRocks identifier may not contain null or"
        " unescaped backtick: {value!r}",
    )  # _validate_identifier: backtick body bad
    STARROCKS_7 = ErrorDef(
        ConnInitException,
        "backtick-quoted StarRocks identifier must be non-empty: {value!r}",
    )  # _validate_identifier: backtick empty
    STARROCKS_8 = ErrorDef(
        ConnInitException,
        "unquoted StarRocks identifier must match [A-Za-z0-9_$]+: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    STARROCKS_9 = ErrorDef(
        ConnInitException,
        "unquoted StarRocks identifier may not be all digits: {value!r}",
    )  # _validate_identifier: all digits
    STARROCKS_10 = ErrorDef(
        ConnInitException,
        "StarRocks table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    STARROCKS_11 = ErrorDef(
        ConnInitException,
        "StarRocks table name must be one of 'table', 'database.table': {value!r}",
    )  # _validate_table_fqn: bad arity
