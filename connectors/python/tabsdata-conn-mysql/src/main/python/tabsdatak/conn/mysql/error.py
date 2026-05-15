from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class MySQLValidateException(ConnException):
    """Cross-field validation failure for the MySQL connector."""


class MySQLRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the MySQL connector."""


class MysqlErrorCode(ErrorCode):
    MYSQL_1 = ErrorDef(
        ConnInitException,
        "MySQLConn validation failed",
    )  # MySQLConn
    MYSQL_2 = ErrorDef(
        ConnInitException,
        "MySQLSrc validation failed",
    )  # MySQLSrc
    MYSQL_3 = ErrorDef(
        MySQLValidateException,
        "MySQLSrc.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # MySQLSrc.validate()
    MYSQL_4 = ErrorDef(
        ConnInitException,
        "MySQLDest validation failed",
    )  # MySQLDest
    MYSQL_5 = ErrorDef(
        ConnInitException,
        "MySQL identifier must be non-empty",
    )  # _validate_identifier: empty
    MYSQL_6 = ErrorDef(
        ConnInitException,
        "MySQL URI must start with mysql:// or mariadb:// (optionally"
        " driver-qualified, e.g. mysql+pymysql://): {value!r}",
    )  # _validate_uri
    MYSQL_7 = ErrorDef(
        MySQLRuntimeException,
        "MySQL source connection failed (uri={uri})",
    )  # _MySQLSrcPlugin.read_in: engine / connect failure
    MYSQL_8 = ErrorDef(
        MySQLRuntimeException,
        "MySQL query failed (query_index={query_index})",
    )  # _MySQLSrcPlugin: read failure inside read_in
    MYSQL_9 = ErrorDef(
        MySQLRuntimeException,
        "MySQL write failed (table={table})",
    )  # _MySQLDestPlugin: write failure inside write_out
    MYSQL_10 = ErrorDef(
        MySQLRuntimeException,
        "MySQLDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _MySQLDestPlugin: defensive recheck
    MYSQL_11 = ErrorDef(
        MySQLRuntimeException,
        "MySQL destination connection failed (uri={uri})",
    )  # _MySQLDestPlugin.write_out: engine / connect failure
    MYSQL_12 = ErrorDef(
        ConnInitException,
        "MySQL identifier exceeds {max} chars: {value!r}",
    )  # _validate_identifier: too long
    MYSQL_13 = ErrorDef(
        ConnInitException,
        "backtick-quoted MySQL identifier may not contain null or"
        " unescaped backtick: {value!r}",
    )  # _validate_identifier: backtick body bad
    MYSQL_14 = ErrorDef(
        ConnInitException,
        "backtick-quoted MySQL identifier must be non-empty: {value!r}",
    )  # _validate_identifier: backtick empty
    MYSQL_15 = ErrorDef(
        ConnInitException,
        "unquoted MySQL identifier must match [A-Za-z0-9_$]+: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    MYSQL_16 = ErrorDef(
        ConnInitException,
        "unquoted MySQL identifier may not be all digits: {value!r}",
    )  # _validate_identifier: all digits
    MYSQL_17 = ErrorDef(
        ConnInitException,
        "MySQL table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    MYSQL_18 = ErrorDef(
        ConnInitException,
        "MySQL table name must be 'table' or 'database.table': {value!r}",
    )  # _validate_table_fqn: bad arity
