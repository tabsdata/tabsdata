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


class MySQLValidateException(ConnException):
    """Cross-field validation failure for the MySQL connector.

    :categories: error
    """


class MySQLRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the MySQL connector.

    :categories: error
    """


class MysqlErrorCode(ErrorCode):
    """Error codes for the MySQL connector.

    :categories: error

    One variant per validation / runtime site (batch and CDC). Raise via
    `MysqlErrorCode.MYSQL_N.exception`.
    """

    MYSQL_1 = ErrorDef(
        ConnInitException,
        "MySQL connection validation failed",
    )  # MySQLSrcConn / MySQLDestConn
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
        "MySQLCdcStreamSrcConn validation failed",
    )  # MySQLCdcStreamSrcConn dataclass tag
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
    )  # unexpected offset type
    MYSQL_18 = ErrorDef(
        ConnInitException,
        "Cannot create TimestampPosition",
    )  # mysql-local TimestampPosition dataclass tag

    # --- Field-value rules (tabsdatak.conn.mysql) -------------------
    MYSQL_19 = ErrorDef(
        ConnInitException,
        "MySQL URI must start with mysql:// or mariadb:// (optionally with a "
        "+driver suffix): {value!r}",
    )  # UriSpec
    MYSQL_20 = ErrorDef(
        MySQLRuntimeException,
        "An initial load needs GTIDs to mark where change capture resumes, but "
        "the server reports gtid_mode={mode!r}. Enable GTIDs, or run without "
        "initial_load",
    )  # open_initial_load
    MYSQL_21 = ErrorDef(
        MySQLRuntimeException,
        "An initial load could not acquire the global read lock that pins the "
        "snapshot to a binlog position, so no snapshot can be taken without "
        "risking lost changes. Grant RELOAD to the connecting user; managed "
        "services that withhold it cannot support initial_load. Cause: {reason}",
    )  # open_initial_load
    MYSQL_22 = ErrorDef(
        MySQLRuntimeException,
        "The server purged binary logs holding transactions the stream has not "
        "read, so the changes in them are lost to it (purged {purged!r}, "
        "consumed {consumed!r}). Run the initial load again",
    )  # BinlogIngester._verify_gtids
    MYSQL_23 = ErrorDef(
        MySQLRuntimeException,
        "The stream has consumed transactions this server never executed "
        "(consumed {consumed!r}, executed {executed!r}): it is another server, "
        "or one restored to an earlier point. Run the initial load again",
    )  # BinlogIngester._verify_gtids
    MYSQL_24 = ErrorDef(
        MySQLRuntimeException,
        "The stream state holds an offset this connector cannot read (it needs "
        "a gtid string and a source string or null): {state!r}. Run the "
        "initial load again",
    )  # GtidOffset.read
    MYSQL_25 = ErrorDef(
        MySQLRuntimeException,
        "Transaction {gtid!r} comes from source {actual!r}, but the stream reads "
        "source {recorded!r}. A stream follows one source server: switching to "
        "another (a failover) or being fed by several is not supported, since "
        "the new source's changes would be dropped as already published. Do "
        "not run the initial load again on this stream, which would drop them "
        "too: start the stream again as a new function",
    )  # BinlogIngester._begin_transaction
    MYSQL_26 = ErrorDef(
        MySQLRuntimeException,
        "The server applies replicated transactions in parallel "
        "(replica_parallel_workers {workers}) without keeping their commit "
        "order (replica_preserve_commit_order {preserved}), so its binlog may "
        "hold a transaction before an earlier one of the same source, which "
        "would be dropped as already published. Set "
        "replica_preserve_commit_order to ON",
    )  # BinlogIngester._verify_server_settings
    MYSQL_27 = ErrorDef(
        MySQLRuntimeException,
        "The server's binlog cannot be streamed as it is set up ({settings}): "
        "the stream needs log_bin ON, binlog_format ROW, binlog_row_image FULL "
        "and gtid_mode ON",
    )  # BinlogIngester._verify_server_settings
    MYSQL_28 = ErrorDef(
        MySQLRuntimeException,
        "The server is a replica that does not write what it replicates to its "
        "own binlog (log_replica_updates {logged}), so the stream would see none "
        "of the changes made on its source. Set log_replica_updates to ON, or "
        "stream from the source",
    )  # BinlogIngester._verify_server_settings
    MYSQL_29 = ErrorDef(
        MySQLRuntimeException,
        "The start timestamp {timestamp} is before the oldest retained binlog, which "
        "begins at {retained}; the changes in between were purged and cannot be "
        "streamed. Start from a later timestamp, from head, or with an initial load",
    )  # BinlogIngester.init_stream
