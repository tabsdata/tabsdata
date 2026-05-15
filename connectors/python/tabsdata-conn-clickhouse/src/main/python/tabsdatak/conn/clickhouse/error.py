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
        ConnInitException,
        "ClickHouse identifier must be non-empty",
    )  # _validate_identifier: empty
    CLICKHOUSE_4 = ErrorDef(
        ConnInitException,
        "ClickHouse URI must start with clickhouse:// (optionally"
        " driver-qualified, e.g. clickhouse+http://): {value!r}",
    )  # _validate_uri
    CLICKHOUSE_5 = ErrorDef(
        ConnInitException,
        "backtick-quoted ClickHouse identifier may not contain null or"
        " unescaped backtick: {value!r}",
    )  # _validate_identifier: backtick body bad chars
    CLICKHOUSE_6 = ErrorDef(
        ConnInitException,
        "backtick-quoted ClickHouse identifier must be non-empty: {value!r}",
    )  # _validate_identifier: backtick empty
    CLICKHOUSE_7 = ErrorDef(
        ConnInitException,
        "double-quoted ClickHouse identifier may not contain null or"
        ' unescaped " : {value!r}',
    )  # _validate_identifier: double-quoted body bad chars
    CLICKHOUSE_8 = ErrorDef(
        ConnInitException,
        "double-quoted ClickHouse identifier must be non-empty: {value!r}",
    )  # _validate_identifier: double-quoted empty
    CLICKHOUSE_9 = ErrorDef(
        ConnInitException,
        "unquoted ClickHouse identifier must match [A-Za-z_][A-Za-z0-9_]*: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    CLICKHOUSE_10 = ErrorDef(
        ConnInitException,
        "ClickHouse table name must be one of 'table', 'database.table': {value!r}",
    )  # _validate_table_fqn: bad arity
