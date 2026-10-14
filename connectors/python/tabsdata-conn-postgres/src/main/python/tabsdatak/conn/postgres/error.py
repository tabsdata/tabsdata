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


class PostgresValidateException(ConnException):
    """Cross-field validation failure for the Postgres connector.

    :categories: error
    """


class PostgresRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Postgres connector.

    :categories: error
    """


class PostgresErrorCode(ErrorCode):
    """Error codes for the Postgres connector.

    :categories: error

    One variant per validation / runtime site (batch and CDC). Raise via
    `PostgresErrorCode.POSTGRES_N.exception`.
    """

    POSTGRES_1 = ErrorDef(
        ConnInitException,
        "Postgres connection validation failed",
    )  # PostgresSrcConn / PostgresDestConn
    POSTGRES_2 = ErrorDef(
        ConnInitException,
        "PostgresSrc validation failed",
    )  # PostgresSrc
    POSTGRES_3 = ErrorDef(
        PostgresValidateException,
        "PostgresSrc.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # PostgresSrc.validate()
    POSTGRES_4 = ErrorDef(
        ConnInitException,
        "PostgresDest validation failed",
    )  # PostgresDest
    POSTGRES_5 = ErrorDef(
        PostgresRuntimeException,
        "Postgres source connection failed (uri={uri})",
    )  # _PostgresSrcPlugin.read_in: engine / connect failure
    POSTGRES_6 = ErrorDef(
        PostgresRuntimeException,
        "Postgres query failed (query_index={query_index})",
    )  # _PostgresSrcPlugin: read failure inside read_in
    POSTGRES_7 = ErrorDef(
        PostgresRuntimeException,
        "PostgresDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _PostgresDestPlugin: defensive recheck
    POSTGRES_8 = ErrorDef(
        PostgresRuntimeException,
        "Postgres destination connection failed (uri={uri})",
    )  # _PostgresDestPlugin.write_out: engine / connect failure
    POSTGRES_9 = ErrorDef(
        PostgresRuntimeException,
        "Postgres write failed (table={table})",
    )  # _PostgresDestPlugin: write failure inside write_out

    # --- CDC --------------------------------------------------------

    POSTGRES_10 = ErrorDef(
        ConnInitException,
        "PostgresCdcStreamSrcConn validation failed",
    )  # PostgresCdcStreamSrcConn dataclass tag
    POSTGRES_11 = ErrorDef(
        ConnInitException,
        "PostgresCdcStreamSrc validation failed",
    )  # PostgresCdcStreamSrc dataclass tag
    POSTGRES_12 = ErrorDef(
        ConnInitException,
        "Cannot create LsnPosition",
    )  # LsnPosition dataclass tag
    POSTGRES_13 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json stream init failed (start_from={start_from!r})",
    )  # Wal2JsonIngester.init_stream: cannot translate start_from
    POSTGRES_14 = ErrorDef(
        PostgresRuntimeException,
        "Postgres replication stream: cannot resolve {position!r} -- "
        "is wal_level=logical and a replication slot available?",
    )  # current_wal_lsn / start_replication: cannot determine position
    POSTGRES_15 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json event: column without a name or type in {payload!r}",
    )  # extract_raw_schema: malformed change payload
    POSTGRES_16 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json offset type mismatch (got {actual!r})",
    )  # unexpected offset type
    POSTGRES_17 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json: unsupported format-version {version!r} (expected 1 or 2)",
    )  # Wal2JsonIngester.poll_changes: unknown format-version
    POSTGRES_18 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json: pending transaction never committed "
        "(xid={xid!r}); slot may be stuck",
    )  # poll_changes_v2: COMMIT timed out

    # --- Field-value rules (tabsdatak.conn.postgres) ----------------
    POSTGRES_19 = ErrorDef(
        ConnInitException,
        "Postgres URI must start with postgres:// or postgresql:// (optionally "
        "with a +driver suffix): {value!r}",
    )  # UriSpec
    POSTGRES_20 = ErrorDef(
        PostgresRuntimeException,
        "Table {schema}.{table} has large text columns {columns} but its REPLICA "
        "IDENTITY is not FULL, so an UPDATE that does not modify one of them "
        "delivers that column as null. Set REPLICA IDENTITY FULL on the table, or "
        "set postgres.cdc.unchanged_large_text_as_null to true to accept the nulls",
    )  # harvest_tables: partial large-text capture
    POSTGRES_22 = ErrorDef(
        PostgresRuntimeException,
        "Replication slot {slot!r} does not exist. An initial load reusing a "
        "slot leaves it to whoever provisioned it, and it must exist before the "
        "load starts or the changes made during the load are not retained. "
        "Create the slot, or set replication_slot_behavior to create",
    )  # open_initial_load
    POSTGRES_23 = ErrorDef(
        PostgresRuntimeException,
        "Replication slot {slot!r} does not exist, so the WAL after the "
        "stream's position {lsn} was not retained for it and the changes in it "
        "are lost to this stream. Recreate the slot and run the initial load "
        "again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_24 = ErrorDef(
        PostgresRuntimeException,
        "Replication slot {slot!r} is invalidated (wal_status lost): the server "
        "removed WAL it needed after the stream's position {lsn}, so the "
        "changes in it are lost to this stream. Recreate the slot and run the "
        "initial load again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_25 = ErrorDef(
        PostgresRuntimeException,
        "Replication slot {slot!r} has confirmed LSN {confirmed}, past the "
        "stream's position {lsn}, so the server would resume after the WAL in "
        "between. This is almost never lost data: while a stream runs, the "
        "replication client moves the slot on by itself over WAL that holds "
        "nothing for the stream, and a stream that ends before recording how "
        "far that went (a killed process, a dropped connection) leaves this "
        "difference behind. It is lost data only if something else moved the "
        "slot while the stream was stopped: another consumer read from it, it "
        "was advanced by hand, or it was dropped and created again. If you are "
        "sure nothing did, set 'postgres.cdc.accept_gap' to {gap!r} in src_cfg "
        "and start the stream again: it resumes at {confirmed}, and the "
        "setting accepts this gap only. Otherwise, run the initial load again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_26 = ErrorDef(
        PostgresRuntimeException,
        "The server's WAL ends at LSN {current}, before the stream's position "
        "{lsn}: the server was restored to an earlier point, and the changes "
        "the stream delivered after it no longer exist there. Run the initial "
        "load again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_27 = ErrorDef(
        PostgresRuntimeException,
        "The stream's position was read from database cluster {recorded}, but "
        "the server is cluster {actual}, where that position means nothing. "
        "Run the initial load again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_28 = ErrorDef(
        PostgresRuntimeException,
        "The stream's position {lsn} is on timeline {recorded}, which the "
        "server's timeline {actual} left {where}: the WAL the stream read past "
        "that point is not in the server's history. Run the initial load again",
    )  # Wal2JsonIngester._verify_resume
    POSTGRES_29 = ErrorDef(
        PostgresRuntimeException,
        "The stream state holds an offset this connector cannot read (it needs "
        "lsn, system_identifier and timeline): {state!r}. Run the initial load "
        "again",
    )  # LsnOffset.read
    POSTGRES_30 = ErrorDef(
        PostgresRuntimeException,
        "wal2json sent transaction {xid!r} without its commit position, which "
        "every row of the transaction is positioned by. The include-lsn option "
        "needs wal2json 2.0 or later",
    )  # Wal2JsonIngester._commit_lsn
    POSTGRES_31 = ErrorDef(
        PostgresRuntimeException,
        "Table {schema}.{table} has no replica identity to log its rows by, so "
        "wal2json would drop its updates and deletes with only a warning in the "
        "server log. Give it a primary key that is not DEFERRABLE, or set "
        "REPLICA IDENTITY FULL or USING INDEX on a unique index",
    )  # Wal2JsonIngester._check_row_identity
    POSTGRES_32 = ErrorDef(
        PostgresRuntimeException,
        "Replication slot {slot!r} is still held by another connection after "
        "{timeout:.0f}s ({reason}): another stream reads it, or a previous one "
        "has not let it go",
    )  # Wal2JsonIngester._attach
    POSTGRES_33 = ErrorDef(
        PostgresValidateException,
        "postgres.cdc.accept_gap is {value!r}, but it takes the gap a POSTGRES_25 "
        "failure names, as '<stream position>-<slot position>' (for example "
        "'0/7173FE8-0/7175C90')",
    )  # accepted_gap
    POSTGRES_34 = ErrorDef(
        PostgresRuntimeException,
        "A {action!r} message arrived at WAL position {lsn} before its transaction's "
        "BEGIN; the stream cannot time it and stops",
    )  # _poll_changes_v2
