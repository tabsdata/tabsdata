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

    # --- CDC --------------------------------------------------------

    MYSQL_10 = ErrorDef(
        ConnInitException,
        "MySQLCdcConn validation failed",
    )  # MySQLCdcConn dataclass tag
    MYSQL_11 = ErrorDef(
        ConnInitException,
        "MySQLCdcStreamSrc validation failed",
    )  # MySQLCdcStreamSrc dataclass tag
    MYSQL_12 = ErrorDef(
        ConnInitException,
        "Cannot create GtidPosition",
    )  # GtidPosition dataclass tag
    MYSQL_13 = ErrorDef(
        ConnInitException,
        "Cannot create BinlogPosition",
    )  # BinlogPosition dataclass tag
    MYSQL_14 = ErrorDef(
        MySQLRuntimeException,
        "MySQL binlog stream init failed (start_from={start_from!r})",
    )  # BinlogIngester.init_stream: cannot translate start_from
    MYSQL_15 = ErrorDef(
        MySQLRuntimeException,
        "MySQL binlog stream: no binary logs available for {position!r}",
    )  # first_binlog_file / last_binlog_position: server reports none
    MYSQL_16 = ErrorDef(
        MySQLRuntimeException,
        "MySQL binlog event: column without a name in {table!r}",
    )  # extract_raw_schema: anonymous column
    MYSQL_17 = ErrorDef(
        MySQLRuntimeException,
        "MySQL binlog offset type mismatch (got {actual!r})",
    )  # commit_offset: unexpected offset type
    MYSQL_18 = ErrorDef(
        ConnInitException,
        "Cannot create TimestampPosition",
    )  # mysql-local TimestampPosition dataclass tag
