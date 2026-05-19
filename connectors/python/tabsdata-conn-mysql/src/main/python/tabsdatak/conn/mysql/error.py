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
        MySQLRuntimeException,
        "MySQL source connection failed (uri={uri})",
    )  # _MySQLSrcPlugin.read_in: engine / connect failure
    MYSQL_6 = ErrorDef(
        MySQLRuntimeException,
        "MySQL query failed (query_index={query_index})",
    )  # _MySQLSrcPlugin: read failure inside read_in
    MYSQL_7 = ErrorDef(
        MySQLRuntimeException,
        "MySQLDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _MySQLDestPlugin: defensive recheck
    MYSQL_8 = ErrorDef(
        MySQLRuntimeException,
        "MySQL destination connection failed (uri={uri})",
    )  # _MySQLDestPlugin.write_out: engine / connect failure
    MYSQL_9 = ErrorDef(
        MySQLRuntimeException,
        "MySQL write failed (table={table})",
    )  # _MySQLDestPlugin: write failure inside write_out
