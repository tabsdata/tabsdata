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
    """Cross-field validation failure for the Postgres connector."""


class PostgresRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Postgres connector."""


class PostgresErrorCode(ErrorCode):
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
    )  # commit_offset / GtidOffset.update: unexpected offset type
    POSTGRES_17 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json: unsupported format-version {version!r} (expected 1 or 2)",
    )  # Wal2JsonIngester.poll_changes: unknown format-version
    POSTGRES_18 = ErrorDef(
        PostgresRuntimeException,
        "Postgres wal2json: pending transaction never committed "
        "(xid={xid!r}); slot may be stuck",
    )  # poll_changes_v2: COMMIT timed out
