#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres CDC implementation -- dialect + wal2json logical-decoding
ingester.

Mirrors the layering of the batch side (``_impl.py``): the connector
front (``PostgresCdcStreamSrcConn`` / ``PostgresCdcStreamSrc`` in
``__init__.py``) and the plugin glue (``_cdc_plugin.py``) sit above
this module; this is where the psycopg2 logical-decoding + sqlalchemy
machinery lives.

Two pieces:

* :class:`PostgresDialect` -- implements the dialect-agnostic
  ``Dialect`` abstract for catalog lookups, identifier quoting,
  large-column classification, and wal2json-type-string ->
  SQLAlchemy mapping (``ddl_to_sa`` over the type strings wal2json
  emits in each change row).
* :class:`Wal2JsonIngester` (+ :class:`LsnOffset`) -- the
  ``CdcIngester`` instance that drives a Postgres logical
  replication slot via ``psycopg2``'s
  ``LogicalReplicationConnection`` and emits ``CdcDataChange``
  records for the runner. Supports wal2json output-plugin format
  versions 1 and 2. Tabsdata owns the resume offset (the LSN is
  persisted in the stream state); the slot is acknowledged once a
  batch is staged, and up to where the server has decoded while
  nothing arrives, so Postgres can release the WAL behind it.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Generator, Iterable, Iterator
from datetime import date, datetime, time as dt_time, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, TypeAlias, Union

import orjson
import psycopg2
import pyarrow as pa
import sqlalchemy as sa
import sqlalchemy.types as sa_types
from psycopg2.extras import LogicalReplicationConnection
from sqlalchemy import URL, make_url, text
from sqlalchemy.dialects.postgresql import OID
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import JSON, LargeBinary, Text

from tabsdatak.conn.common._stream._offsets import TabsdataKeptOffsets

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import DEFAULT_POLL_MAX_SECS

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._ingester import (
    CDC_OFFSET_STATE_KEY,
    CdcIngester,
)
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    InitialLoadSession,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._tz import parse_timezone
from tabsdatak.conn.common.sql.cdc.constants import (
    DOT,
    CdcOperation,
)
from tabsdatak.conn.common.sql.cdc.dialect import (
    CdcIngesterType,
    Dialect,
    LargeColumnKind,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat, QualifiedTablesSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.postgres import LsnPosition
from tabsdatak.conn.postgres.error import PostgresErrorCode

logger = logging.getLogger(__name__)


# --- Postgres-specific runtime constants ---------------------------

# wal2json's logical-decoding output plugin.
WAL2JSON_PLUGIN = "wal2json"

DEFAULT_REPLICATION_SLOT_BEHAVIOR = "reuse"

# Loop cadence inside the wal2json reader when the cursor has no
# message available -- cheap sleeps so the runner can interrupt.
POLL_INTERVAL_SECONDS = 1.0

# When a transaction has begun (v2 BEGIN seen) but no COMMIT has
# arrived yet, the poll outlives its deadline rather than synthesize
# a half-applied transaction into the staging area: it waits this
# many poll budgets for the NEXT message, however long the whole
# transaction takes to read. Only a source gone silent mid-transaction
# fails the stream.
PENDING_TIMEOUT_MULTIPLIER = 16

# A stream restarted right after a stop can find its slot still held: the
# previous pod's walsender releases it only once it notices the connection
# is gone. Attaching is retried on the same connection for this long.
SLOT_ATTACH_TIMEOUT_SECONDS = 30.0

# While the stream holds the slot its confirmed position is read back this
# often and recorded, in case something other than the stream's own feedback
# moved it.
SLOT_WATCH_INTERVAL_SECONDS = 10.0

# A clean stop reads the slot once the server has released it, which it does
# after applying every feedback message the stream sent.
SLOT_RELEASE_TIMEOUT_SECONDS = 10.0

DEFAULT_WAL2JSON_FORMAT_VERSION: int = 2

REPLICA_IDENTITY_FULL = "f"


# Internal start-position shape consumed by the ingester after the
# plugin has converted the user-facing ``PostgresCdcStartFromSpec``.
_InternalStartFromSpec: TypeAlias = Union[
    Literal["head", "tail"],
    LsnPosition,
]


# wal2json v1 reports the operation in the change row's ``kind``
# field as a lower-case verb; v2 reports it in the action message's
# ``action`` field as a single capital letter.
_V1_OPERATION_MAP = {
    "insert": CdcOperation.INSERT,
    "update": CdcOperation.UPDATE,
    "delete": CdcOperation.DELETE,
}


class _V2Actions:
    BEGIN = "B"
    COMMIT = "C"
    TRUNCATE = "T"
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


_V2_OPERATION_MAP = {
    _V2Actions.INSERT: CdcOperation.INSERT,
    _V2Actions.UPDATE: CdcOperation.UPDATE,
    _V2Actions.DELETE: CdcOperation.DELETE,
}


# ---------------------------------------------------------------------
# Dialect
# ---------------------------------------------------------------------

_LARGE_OBJECT_TYPES = (LargeBinary, OID)
_LARGE_TEXT_TYPES = (Text, JSON)


# wal2json type strings are Postgres DDL-shaped (``integer``,
# ``character varying(64)``, ``numeric(10,2)``, ``timestamp with
# time zone``, ``boolean``, ...). The runner builds the PyArrow
# schema by going through SQLAlchemy, so we map the DDL string
# into the most-specific SA type the downstream consumer can use.
_WAL2JSON_PRIMITIVE_TYPES: dict[str, type] = {
    "boolean": sa_types.Boolean,
    "bool": sa_types.Boolean,
    "smallint": sa_types.SmallInteger,
    "int2": sa_types.SmallInteger,
    "integer": sa_types.Integer,
    "int": sa_types.Integer,
    "int4": sa_types.Integer,
    "bigint": sa_types.BigInteger,
    "int8": sa_types.BigInteger,
    "real": sa_types.Float,
    "float4": sa_types.Float,
    "double precision": sa_types.Float,
    "float8": sa_types.Float,
    "text": sa_types.Text,
    "uuid": sa_types.String,
    "json": sa_types.JSON,
    "jsonb": sa_types.JSON,
    "bytea": sa_types.LargeBinary,
    "date": sa_types.Date,
}


def _parse_wal2json_type(dtype: str) -> sa_types.TypeEngine:
    """Map a wal2json column-type string onto a SQLAlchemy type.
    Handles parameterised types (``numeric(10,2)`` / ``varchar(64)``
    / timestamp variants) and falls back to a bare string."""
    raw = (dtype or "").strip().lower()
    if not raw:
        return sa_types.String()
    base, _, rest = raw.partition("(")
    base = base.strip()
    args: list[str] = []
    if rest:
        args = [a.strip() for a in rest.rstrip(")").split(",") if a.strip()]
    if base in ("character varying", "varchar"):
        if args:
            try:
                return sa_types.String(length=int(args[0]))
            except ValueError:
                pass
        return sa_types.String()
    if base in ("character", "char", "bpchar"):
        if args:
            try:
                return sa_types.String(length=int(args[0]))
            except ValueError:
                pass
        return sa_types.String()
    if base in ("numeric", "decimal"):
        kw: dict[str, Any] = {}
        if len(args) >= 1:
            try:
                kw["precision"] = int(args[0])
            except ValueError:
                pass
        if len(args) >= 2:
            try:
                kw["scale"] = int(args[1])
            except ValueError:
                pass
        return sa_types.Numeric(**kw)
    if base.startswith("timestamp"):
        return sa_types.DateTime(timezone="with time zone" in raw)
    if base == "time" or base.startswith("time "):
        return sa_types.Time(timezone="with time zone" in raw)
    klass = _WAL2JSON_PRIMITIVE_TYPES.get(base)
    if klass is not None:
        return klass()
    # Unknown DDL string -> bare string. Logs at debug so a future
    # type addition is discoverable but doesn't spam in production.
    logger.debug("wal2json type %r mapped to fallback String", dtype)
    return sa_types.String()


class PostgresDialect(Dialect):
    """``Dialect`` for the Postgres wal2json CDC ingester."""

    @property
    def open_quote(self) -> str:
        return '"'

    @property
    def close_quote(self) -> str:
        return '"'

    def default_case_fold(self, name: str) -> str:
        # Postgres folds unquoted identifiers to lower case.
        return name.lower()

    def parse_qualified_table(
        self, tablespec: str
    ) -> tuple[quoted_name | None, quoted_name]:
        spec = tablespec.strip()
        if not spec:
            raise ValueError(f"expected 'schema.table' or 'table', got {tablespec!r}")

        def _unquote(part: str) -> tuple[str, bool]:
            if part.startswith('"') and part.endswith('"') and len(part) >= 2:
                return part[1:-1].replace('""', '"'), True
            return part, False

        # Unqualified: the schema is filled from the connection's default
        # schema (current_schema()) by the caller.
        if "." not in spec:
            table, table_quoted = _unquote(spec)
            if not table:
                raise ValueError(f"empty table in {tablespec!r}")
            return None, quoted_name(table, quote=table_quoted)

        schema_part, _, table_part = spec.partition(".")
        schema, schema_quoted = _unquote(schema_part)
        table, table_quoted = _unquote(table_part)
        if not schema or not table:
            raise ValueError(f"empty schema or table in {tablespec!r}")
        return (
            quoted_name(schema, quote=schema_quoted),
            quoted_name(table, quote=table_quoted),
        )

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(text("show timezone")).fetchone()
        if row and row[0] is not None:
            return parse_timezone(row[0])
        return None

    def large_column_kind(self, satype: Any) -> LargeColumnKind | None:
        if isinstance(satype, _LARGE_OBJECT_TYPES):
            return "object"
        if isinstance(satype, _LARGE_TEXT_TYPES):
            return "text"
        return None

    def replica_identity(self, connection: Any, schema: str, table: str) -> str:
        return connection.execute(
            text(
                "select pg_catalog.pg_class.relreplident "
                "from pg_catalog.pg_class "
                "join pg_catalog.pg_namespace "
                "on pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid "
                "where pg_catalog.pg_namespace.nspname = :schema "
                "and pg_catalog.pg_class.relname = :table"
            ),
            {"schema": schema, "table": table},
        ).scalar()

    def logs_row_identity(self, connection: Any, schema: str, table: str) -> bool:
        """Whether an UPDATE or DELETE of the table is logged with the row it
        applies to. Without one, wal2json sends neither, with only a warning
        in the server log: ``REPLICA IDENTITY NOTHING``, the default with no
        primary key or a ``DEFERRABLE`` one (which cannot serve as identity),
        and ``USING INDEX`` once that index is gone."""
        row = connection.execute(
            text(
                "select c.relreplident, "
                "exists (select 1 from pg_catalog.pg_index i "
                "join pg_catalog.pg_constraint k on k.conindid = i.indexrelid "
                "where i.indrelid = c.oid and i.indisprimary "
                "and not k.condeferrable), "
                "exists (select 1 from pg_catalog.pg_index i "
                "where i.indrelid = c.oid and i.indisreplident) "
                "from pg_catalog.pg_class c "
                "join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
                "where n.nspname = :schema and c.relname = :table"
            ),
            {"schema": schema, "table": table},
        ).fetchone()
        if row is None:
            return False
        identity, primary_key, identity_index = row
        match identity:
            case "f":
                return True
            case "d":
                return bool(primary_key)
            case "i":
                return bool(identity_index)
            case _:
                return False

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select a.attname "
                "from pg_catalog.pg_attribute a "
                "join pg_catalog.pg_class c "
                "on a.attrelid = c.oid "
                "join pg_catalog.pg_namespace n "
                "on c.relnamespace = n.oid "
                "where n.nspname = :schema "
                "and c.relname = :table "
                "and a.attnum > 0 "
                "and not a.attisdropped "
                "order by a.attnum"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
        return [row[0] for row in rows]

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        if ingester_type == "wal2json":
            return _parse_wal2json_type(str(cdc_dtype))
        raise NotImplementedError(
            f"{type(self).__name__}.cdc_to_sa not implemented for "
            f"ingester_type={ingester_type!r}"
        )


# ---------------------------------------------------------------------
# Offset
# ---------------------------------------------------------------------


def lsn_to_int(lsn: str) -> int:
    high, _, low = lsn.partition("/")
    return (int(high, 16) << 32) | int(low, 16)


def lsn_to_text(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def gap_to_text(start: int, end: int) -> str:
    return f"{lsn_to_text(start)}-{lsn_to_text(end)}"


def accepted_gap(value: Any) -> tuple[int, int] | None:
    """The gap `value` accepts, written as `gap_to_text` writes it."""
    if value is None:
        return None
    try:
        start, end = str(value).split("-")
        return lsn_to_int(start.strip()), lsn_to_int(end.strip())
    except ValueError as error:
        raise PostgresErrorCode.POSTGRES_33.exception(cause=error, value=value)


def identify_system(cursor: Any) -> tuple[str, int]:
    """The database cluster's system identifier and current timeline, read
    with ``IDENTIFY_SYSTEM`` on a replication connection's `cursor`."""
    cursor.execute("IDENTIFY_SYSTEM")
    system_identifier, timeline, _, _ = cursor.fetchone()
    return str(system_identifier), int(timeline)


class Wal2JsonInitialLoadSession(InitialLoadSession):
    def __init__(self, *, engine: Any):
        super().__init__()
        self._engine = engine
        self._offset: LsnOffset | None = None
        self._snapshot_name: str | None = None

    @property
    def engine(self) -> Any:
        return self._engine

    @property
    def offset(self) -> CdcOffset:
        return self._offset

    def tables(
        self, tracked_tables: dict[str, TrackedTable]
    ) -> Iterator[tuple[TrackedTable, Any]]:
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ", postgresql_readonly=True
        ) as connection:
            connection.execute(
                text(f"set transaction snapshot '{self._snapshot_name}'")
            )
            self._timestamp = (
                connection
                .execute(text("select now()"))
                .scalar()
                .astimezone(timezone.utc)
            )
            for tracked_table in tracked_tables.values():
                yield tracked_table, connection

    def close(self) -> None:
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose initial-load engine: %s", exc)
            self._engine = None


class SlotSnapshotSession(Wal2JsonInitialLoadSession):
    """Loads from the snapshot the server exports as it creates a slot,
    consistent at the point the slot streams from: no change is both
    loaded and streamed, and none is missed. The slot is the stream's own
    when the load creates it, and otherwise a temporary one beside the
    stream's, which the server drops with the connection: when the stream
    reuses its slot, or when the slot the load was to create is already
    there, as after an attempt that died before staging. A snapshot taken
    beside the slot between two readings of the WAL position would instead
    miss a transaction whose commit is written but not yet visible, for as
    long as a synchronous standby takes to answer: before the first
    reading, so never streamed, and not in the snapshot."""

    def __init__(
        self,
        *,
        engine: Any,
        replication_settings: dict[str, Any],
        replication_slot: str,
        create: bool,
    ):
        super().__init__(engine=engine)
        self._replicator = psycopg2.connect(**replication_settings)
        self._cursor = self._replicator.cursor()
        # Before the slot: its exported snapshot lasts only until the next
        # command on this connection.
        system_identifier, timeline = identify_system(self._cursor)
        consistent_point, snapshot_name = self._export_snapshot(
            replication_slot, create
        )
        self._offset = LsnOffset(
            lsn=lsn_to_int(consistent_point),
            system_identifier=system_identifier,
            timeline=timeline,
        )
        self._snapshot_name = snapshot_name
        logger.info(
            "Initial load snapshot of slot %r is consistent at %s",
            replication_slot,
            consistent_point,
        )

    def _export_snapshot(self, replication_slot: str, create: bool) -> tuple[str, str]:
        if create:
            try:
                self._cursor.execute(
                    f"CREATE_REPLICATION_SLOT {replication_slot} LOGICAL "
                    f"{WAL2JSON_PLUGIN} EXPORT_SNAPSHOT"
                )
                return self._exported()
            except psycopg2.errors.DuplicateObject:
                logger.info(
                    "Replication slot %r already exists; the initial load "
                    "snapshots beside it and the stream resumes it",
                    replication_slot,
                )
        elif not self._slot_exists(replication_slot):
            raise PostgresErrorCode.POSTGRES_22.exception(slot=replication_slot)
        self._cursor.execute(
            f"CREATE_REPLICATION_SLOT {_snapshot_slot(replication_slot)} TEMPORARY "
            f"LOGICAL {WAL2JSON_PLUGIN} EXPORT_SNAPSHOT"
        )
        return self._exported()

    def _exported(self) -> tuple[str, str]:
        _, consistent_point, snapshot_name, _ = self._cursor.fetchone()
        return consistent_point, snapshot_name

    def _slot_exists(self, replication_slot: str) -> bool:
        with self._engine.connect() as connection:
            return bool(
                connection.execute(
                    text("select 1 from pg_replication_slots where slot_name = :slot"),
                    {"slot": replication_slot},
                ).scalar()
            )

    def close(self) -> None:
        for closeable, what in (
            (self._cursor, "cursor"),
            (self._replicator, "replication connection"),
        ):
            if closeable is None:
                continue
            try:
                closeable.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close initial-load %s: %s", what, exc)
        self._cursor = None
        self._replicator = None
        super().close()


SLOT_NAME_MAX_LENGTH = 63
SNAPSHOT_SLOT_SUFFIX = "_snapshot"


def _snapshot_slot(replication_slot: str) -> str:
    """The name of the temporary slot a load exports its snapshot from,
    beside the slot the stream reuses."""
    keep = SLOT_NAME_MAX_LENGTH - len(SNAPSHOT_SLOT_SUFFIX)
    return replication_slot[:keep] + SNAPSHOT_SLOT_SUFFIX


class LsnOffset(CdcOffset):
    """Postgres CDC offset = the WAL LSN the consumer has flushed
    so far. Zero is the cold-start marker (replay from the oldest
    WAL the slot retains).

    It also records the server whose WAL the LSN belongs to -- its system
    identifier and timeline -- so a resume can tell that server from a
    restore of it or from another cluster."""

    def __init__(
        self,
        lsn: int = 0,
        system_identifier: str | None = None,
        timeline: int | None = None,
    ):
        self.lsn = lsn
        self.system_identifier = system_identifier
        self.timeline = timeline

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, LsnOffset):
            raise PostgresErrorCode.POSTGRES_16.exception(
                actual=type(other).__name__,
            )
        self.lsn = other.lsn
        if other.system_identifier is not None:
            self.system_identifier = other.system_identifier
            self.timeline = other.timeline

    def write(self) -> str:
        return json.dumps({
            "lsn": self.lsn,
            "system_identifier": self.system_identifier,
            "timeline": self.timeline,
        })

    @classmethod
    def read(cls, data: str) -> "LsnOffset":
        try:
            payload = json.loads(data)
            lsn = payload["lsn"]
            system_identifier = payload["system_identifier"]
            timeline = payload["timeline"]
            if not (
                isinstance(lsn, int)
                and isinstance(system_identifier, str)
                and isinstance(timeline, int)
            ):
                raise TypeError("lsn, system_identifier or timeline mistyped")
        except (TypeError, ValueError, KeyError) as error:
            raise PostgresErrorCode.POSTGRES_29.exception(cause=error, state=data)
        return cls(lsn=lsn, system_identifier=system_identifier, timeline=timeline)

    def copy(self) -> "LsnOffset":
        return LsnOffset(
            lsn=self.lsn,
            system_identifier=self.system_identifier,
            timeline=self.timeline,
        )


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class Wal2JsonIngester(CdcIngester):
    """``CdcIngester`` for Postgres via wal2json logical decoding.

    Opens a SQLAlchemy engine for catalog introspection plus a
    separate ``psycopg2.LogicalReplicationConnection`` for the
    change stream. Polled by ``run_cdc``; emits ``CdcDataChange``
    per row event.

    The wal2json output plugin has two protocol versions:

    * v1 -- one message per transaction; the ``change`` array
      carries every row event for that xid.
    * v2 (default) -- one message per action with explicit
      ``B`` (begin) / ``C`` (commit) framing.
    """

    def __init__(
        self,
        # The ``Conn`` is duck-typed: the ingester only reads
        # ``uri`` / ``credentials`` / ``conn_cfg``. Concrete type is
        # PostgresCdcStreamSrcConn but we don't import it here to avoid the
        # __init__.py -> _cdc_impl.py cycle.
        conn: Any,
        tables: QualifiedTablesSpec,
        start_from: _InternalStartFromSpec,
        replication_slot: str,
        replication_slot_behavior: str = DEFAULT_REPLICATION_SLOT_BEHAVIOR,
        poll_max_secs: int = DEFAULT_POLL_MAX_SECS,
        publication_name: str | None = None,
        wal2json_format_version: int = DEFAULT_WAL2JSON_FORMAT_VERSION,
        unchanged_large_text_as_null: bool = False,
        accepted_gap: tuple[int, int] | None = None,
    ):
        super().__init__(tables=tables)
        self.offsets = TabsdataKeptOffsets(
            key=CDC_OFFSET_STATE_KEY,
            read=LsnOffset.read,
            acknowledger=self.confirm_lsn,
        )
        self._conn = conn
        self._start_from = start_from
        self._replication_slot = replication_slot
        self._replication_slot_behavior = replication_slot_behavior
        self._poll_max_secs = poll_max_secs
        self._publication_name = publication_name
        self._wal2json_format_version = wal2json_format_version
        self._unchanged_large_text_as_null = unchanged_large_text_as_null
        self._mark: LsnOffset | None = None
        self._base_offset: LsnOffset | None = None
        self._replicator: Any | None = None
        self._cursor: Any | None = None
        self._bound: int | None = None
        self._bound_reached = False
        self._held: list[Any] = []
        self._accepted_gap = accepted_gap
        # The furthest position the slot is known to have been confirmed at
        # by this stream, and the furthest one handed on to be recorded:
        # the recorded offset is kept at or past the first, so a resume
        # never finds the slot ahead of it for a move of the stream's own.
        self._slot_floor = 0
        self._recorded = 0
        # The furthest position confirmed to the server, and how many changes
        # have been handed out since the last confirmation: while nothing
        # arrives, the position the server has decoded up to is confirmed
        # only once every change handed out is staged, which the next
        # confirmation says.
        self._confirmed = 0
        self._unacknowledged = 0
        self._watched_at = 0.0
        self._watcher: Any | None = None

    @property
    def ingester_type(self) -> CdcIngesterType:
        return "wal2json"

    @property
    def dialect(self) -> PostgresDialect:
        return PostgresDialect()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(self, work_dir: Path) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("PostgreSQL server timezone: %s", self._server_tz)
        self.init_stream()
        return engine

    def harvest_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        tracked_tables = super().harvest_tables(session=session, cdc_format=cdc_format)
        self._check_row_identity(session, tracked_tables)
        self._check_partial_text_capture(session, tracked_tables)
        return tracked_tables

    def _check_row_identity(
        self,
        engine: Any,
        tracked_tables: dict[str, TrackedTable],
    ) -> None:
        with engine.connect() as connection:
            for tracked in tracked_tables.values():
                schema = tracked.data_table.schema
                table = tracked.data_table.table
                if not self.dialect.logs_row_identity(connection, schema, table):
                    raise PostgresErrorCode.POSTGRES_31.exception(
                        schema=schema, table=table
                    )

    def _check_partial_text_capture(
        self,
        engine: Any,
        tracked_tables: dict[str, TrackedTable],
    ) -> None:
        # Without REPLICA IDENTITY FULL, an UPDATE that leaves a TOASTed
        # value untouched carries no copy of it in the WAL, so the column
        # arrives as null. That is only acceptable when the user opted in
        # with ``unchanged_large_text_as_null``; otherwise the stream
        # refuses to start.
        inspector = sa.inspect(engine)
        with engine.connect() as connection:
            for tracked in tracked_tables.values():
                schema = tracked.data_table.schema
                table = tracked.data_table.table
                text_columns = [
                    column["name"]
                    for column in inspector.get_columns(table, schema=schema)
                    if self.dialect.large_column_kind(column["type"]) == "text"
                ]
                if not text_columns:
                    continue
                identity = self.dialect.replica_identity(connection, schema, table)
                if identity == REPLICA_IDENTITY_FULL:
                    continue
                if not self._unchanged_large_text_as_null:
                    raise PostgresErrorCode.POSTGRES_20.exception(
                        schema=schema,
                        table=table,
                        columns=text_columns,
                    )
                logger.warning(
                    "Table %s.%s has large text columns %s but its REPLICA "
                    "IDENTITY is not FULL: an UPDATE that does not modify one "
                    "of them delivers that column as null",
                    schema,
                    table,
                    text_columns,
                )

    def open_initial_load(self) -> Wal2JsonInitialLoadSession:
        return SlotSnapshotSession(
            engine=self._build_engine(),
            replication_settings=self._replication_connection_settings(),
            replication_slot=self._replication_slot,
            create=self._replication_slot_behavior == "create",
        )

    def close_session(self, session: Any) -> None:
        if self._cursor is not None:
            try:
                self._cursor.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close cursor: %s", exc)
            finally:
                self._cursor = None
        if self._replicator is not None:
            try:
                self._replicator.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close replicator: %s", exc)
            finally:
                self._replicator = None
        self._close_watcher()
        if session is not None:
            try:
                session.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose SA engine: %s", exc)

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _resolve_url(self) -> URL:
        url = make_url(self._conn._uri())
        creds = self._conn.credentials
        return url.set(
            username=_convert(creds.user, str),
            password=_convert(creds.password, str),
        )

    def _build_engine(self) -> Any:
        return sqlalchemy_utils.create_engine(
            self._resolve_url(), connect_args=self.connect_args
        )

    def _replication_connection_settings(self) -> dict[str, Any]:
        url = self._resolve_url()
        settings: dict[str, Any] = {
            **self.connect_args,
            "host": url.host,
            "port": url.port or 5432,
            "dbname": url.database,
            "connection_factory": LogicalReplicationConnection,
        }
        if url.username:
            settings["user"] = url.username
        if url.password:
            settings["password"] = url.password
        return settings

    # ------------------------------------------------------------------
    # Stream init -- translate start_from into wal2json kwargs
    # ------------------------------------------------------------------

    def init_stream(self) -> None:
        settings = self._replication_connection_settings()
        self._replicator = psycopg2.connect(**settings)
        self._cursor = self._replicator.cursor()
        if self._replication_slot_behavior == "create":
            try:
                self._cursor.create_replication_slot(
                    self._replication_slot,
                    output_plugin=WAL2JSON_PLUGIN,
                )
                logger.info("Created replication slot %r", self._replication_slot)
            except psycopg2.errors.DuplicateObject:
                logger.info(
                    "Replication slot %r already exists; reusing it",
                    self._replication_slot,
                )
        else:
            logger.info("Reusing replication slot %r", self._replication_slot)

        if self._mark is None:
            match self._start_from:
                case LsnPosition(lsn=lsn):
                    self._mark = LsnOffset(lsn=lsn)
                case "head":
                    self._mark = LsnOffset(lsn=0)
                case "tail":
                    self._mark = LsnOffset(lsn=self._current_wal_lsn())
                case _:
                    raise PostgresErrorCode.POSTGRES_13.exception(
                        start_from=self._start_from,
                    )

        system_identifier, timeline = identify_system(self._cursor)
        self._recorded = self._mark.lsn
        self._confirmed = 0
        self._unacknowledged = 0
        self._verify_resume(system_identifier, timeline)
        self._mark.system_identifier = system_identifier
        self._mark.timeline = timeline

        options: dict[str, str] = {
            "format-version": str(self._wal2json_format_version),
            "include-transaction": "true",
            "include-xids": "true",
            "include-timestamp": "true",
            # Puts the commit position on each format-2 BEGIN message, which
            # every row of the transaction then takes as its segment.
            "include-lsn": "true",
            # Numbers come as decimal strings, not JSON numbers: a JSON number
            # is parsed as a float and a numeric loses every digit past the
            # fifteenth before the Decimal is built from it.
            "numeric-data-types-as-string": "true",
            "add-tables": self._schema_table_filter(),
        }
        if self._publication_name:
            options["publication_names"] = self._publication_name

        self._base_offset = self._mark.copy()
        self._attach(options)
        logger.info("Replication slot %r attached", self._replication_slot)
        self._slot_floor = self._mark.lsn
        self._watched_at = time.monotonic()
        if self._mark.lsn:
            self.confirm_lsn(self._mark)
        self._record()

    def _attach(self, options: dict[str, str]) -> None:
        deadline = time.monotonic() + SLOT_ATTACH_TIMEOUT_SECONDS
        while True:
            try:
                self._cursor.start_replication(
                    slot_name=self._replication_slot,
                    decode=True,
                    start_lsn=self._mark.lsn,
                    options=options,
                )
                return
            except psycopg2.errors.ObjectInUse as error:
                if time.monotonic() >= deadline:
                    raise PostgresErrorCode.POSTGRES_32.exception(
                        cause=error,
                        slot=self._replication_slot,
                        timeout=SLOT_ATTACH_TIMEOUT_SECONDS,
                        reason=str(error).strip(),
                    )
                logger.info(
                    "Replication slot %r is still held; retrying: %s",
                    self._replication_slot,
                    str(error).strip(),
                )
                time.sleep(POLL_INTERVAL_SECONDS)

    def _verify_resume(self, system_identifier: str, timeline: int) -> None:
        """Fail unless the server still holds every change after the stream's
        position: the same cluster, on a timeline whose history contains
        that position, with the slot retaining the WAL from it. Asking the
        slot to start before its confirmed position is not an error on the
        server's side: it silently starts from that position instead."""
        mark = self._mark
        if mark.system_identifier is not None:
            if mark.system_identifier != system_identifier:
                raise PostgresErrorCode.POSTGRES_27.exception(
                    recorded=mark.system_identifier,
                    actual=system_identifier,
                )
            if mark.timeline != timeline:
                switch = self._timeline_switch(mark.timeline, timeline)
                if switch is None or mark.lsn > switch:
                    raise PostgresErrorCode.POSTGRES_28.exception(
                        lsn=lsn_to_text(mark.lsn),
                        recorded=mark.timeline,
                        actual=timeline,
                        where=(
                            f"at LSN {lsn_to_text(switch)}"
                            if switch is not None
                            else "at no point its history records"
                        ),
                    )
        if not mark.lsn:
            return
        positions = self._slot_positions()
        if positions is None:
            raise PostgresErrorCode.POSTGRES_23.exception(
                slot=self._replication_slot,
                lsn=lsn_to_text(mark.lsn),
            )
        confirmed, wal_status, current = positions
        if wal_status == "lost":
            raise PostgresErrorCode.POSTGRES_24.exception(
                slot=self._replication_slot,
                lsn=lsn_to_text(mark.lsn),
            )
        if confirmed is None:
            raise PostgresErrorCode.POSTGRES_25.exception(
                slot=self._replication_slot,
                confirmed="none",
                lsn=lsn_to_text(mark.lsn),
                gap="none",
            )
        if int(confirmed) > mark.lsn:
            self._skip_gap(mark, int(confirmed))
        if int(current) < mark.lsn:
            raise PostgresErrorCode.POSTGRES_26.exception(
                current=lsn_to_text(int(current)),
                lsn=lsn_to_text(mark.lsn),
            )

    def _skip_gap(self, mark: LsnOffset, confirmed: int) -> None:
        """Resume at `confirmed`, past the stream's position, only when the
        configuration accepts that very gap."""
        gap = gap_to_text(mark.lsn, confirmed)
        if self._accepted_gap != (mark.lsn, confirmed):
            raise PostgresErrorCode.POSTGRES_25.exception(
                slot=self._replication_slot,
                confirmed=lsn_to_text(confirmed),
                lsn=lsn_to_text(mark.lsn),
                gap=gap,
            )
        logger.warning(
            "Replication slot %r is at %s, past the stream's position %s: "
            "resuming there, as the configuration accepts the gap %s. Any "
            "change to the tracked tables in it is not delivered",
            self._replication_slot,
            lsn_to_text(confirmed),
            lsn_to_text(mark.lsn),
            gap,
        )
        mark.lsn = confirmed

    def _timeline_switch(self, recorded: int, current: int) -> int | None:
        """The LSN at which the server's history left timeline `recorded`, or
        ``None`` when that history does not pass through it."""
        try:
            self._cursor.execute(f"TIMELINE_HISTORY {current}")
            _, content = self._cursor.fetchone()
        except psycopg2.Error:
            return None
        if isinstance(content, (bytes, memoryview)):
            content = bytes(content).decode()
        for line in content.splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0].isdigit() and int(fields[0]) == recorded:
                return lsn_to_int(fields[1])
        return None

    def _slot_positions(self) -> tuple[Any, str | None, Any] | None:
        """The slot's confirmed LSN and WAL status with the server's current
        LSN, or ``None`` when the slot does not exist. ``to_jsonb`` reads
        ``wal_status`` without failing on a server too old to have it."""
        settings = dict(self._replication_connection_settings())
        settings.pop("connection_factory", None)
        conn = psycopg2.connect(**settings)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "select confirmed_flush_lsn - '0/0'::pg_lsn, "
                    "to_jsonb(slot) ->> 'wal_status', "
                    "pg_current_wal_lsn() - '0/0'::pg_lsn "
                    "from pg_replication_slots slot where slot_name = %s",
                    (self._replication_slot,),
                )
                return cursor.fetchone()
        finally:
            conn.close()

    def _current_wal_lsn(self) -> int:
        # Open a regular psycopg2 connection (not a logical
        # replication one) to query ``pg_current_wal_lsn``. The
        # subtraction with ``'0/0'::pg_lsn`` is the cheapest way to
        # get the LSN as a 64-bit integer.
        settings = dict(self._replication_connection_settings())
        settings.pop("connection_factory", None)
        conn = psycopg2.connect(**settings)
        try:
            with conn.cursor() as cursor:
                cursor.execute("select pg_current_wal_lsn() - '0/0'::pg_lsn")
                row = cursor.fetchone()
                if not row or row[0] is None:
                    raise PostgresErrorCode.POSTGRES_14.exception(position="tail")
                return int(row[0])
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Table filter
    # ------------------------------------------------------------------

    def _schema_table_filter(self) -> str:
        # wal2json ``add-tables`` accepts ``"schema.table[,schema.table...]"``
        # or ``"*.*"`` for "everything in the tracked database".
        if not self._tracked_tables:
            return "*.*"
        tables = [
            f"{tt.data_table.schema}{DOT}{tt.data_table.table}"
            for tt in self._tracked_tables.values()
        ]
        return ",".join(tables)

    def _as_qualified_table(self, schema: str, table: str) -> str | None:
        qualified = f"{schema}{DOT}{table}"
        if qualified in self._tracked_tables:
            return qualified
        return None

    # ------------------------------------------------------------------
    # Offset management
    # ------------------------------------------------------------------

    def base_offset(self) -> CdcOffset:
        return (self._base_offset or LsnOffset()).copy()

    def confirm_lsn(self, offset: LsnOffset) -> None:
        if self._cursor is None:  # pragma: no cover -- stream init failed
            return
        self._cursor.send_feedback(flush_lsn=offset.lsn, reply=True)
        self._confirmed = max(self._confirmed, offset.lsn)
        self._unacknowledged = 0
        logger.debug("Confirmed LSN offset %d", offset.lsn)

    def _confirm_idle(self) -> None:
        if self._cursor is None or self._bound is not None or self._unacknowledged:
            return
        wal_end = self._cursor.wal_end
        if wal_end <= self._confirmed:
            return
        offset = self.base_offset()
        offset.lsn = wal_end
        if not self.lifecycle.checkpoint(offset):
            return
        self._slot_floor = self._recorded = wal_end
        self.confirm_lsn(offset)

    def staged_offset(self, offset: CdcOffset) -> CdcOffset:
        if offset.lsn < self._slot_floor:
            offset = offset.copy()
            offset.lsn = self._slot_floor
        self._recorded = max(self._recorded, offset.lsn)
        return offset

    def settle_offset(self) -> None:
        """Record where the slot ends up once this stream lets it go. The
        server releases it after applying every feedback message the
        stream sent, so its position then is final; releasing is the one
        way this can be told from outside."""
        if self._replicator is None:
            return
        pid = self._replicator.get_backend_pid()
        self._cursor.close()
        self._replicator.close()
        self._cursor = None
        self._replicator = None
        deadline = time.monotonic() + SLOT_RELEASE_TIMEOUT_SECONDS
        while True:
            row = self._slot_row()
            if row is None:
                return
            confirmed, active_pid = row
            if active_pid is None:
                break
            if active_pid != pid:
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "Replication slot %r was not released within %.0fs; its "
                    "final offset is not recorded",
                    self._replication_slot,
                    SLOT_RELEASE_TIMEOUT_SECONDS,
                )
                return
            time.sleep(POLL_INTERVAL_SECONDS / 10)
        if confirmed is not None:
            self._raise_floor(int(confirmed))

    def _watch_slot(self) -> None:
        """Every so often, read how far the slot is confirmed while this
        stream holds it, and record any move the stream did not make."""
        now = time.monotonic()
        if (
            self._replicator is None
            or now - self._watched_at < SLOT_WATCH_INTERVAL_SECONDS
        ):
            return
        self._watched_at = now
        try:
            row = self._slot_row()
        except psycopg2.Error as error:
            logger.warning("Could not read replication slot position: %s", error)
            self._close_watcher()
            return
        if row is None:
            return
        confirmed, active_pid = row
        if confirmed is not None and active_pid == self._replicator.get_backend_pid():
            self._raise_floor(int(confirmed))

    def _raise_floor(self, confirmed: int) -> None:
        if confirmed > self._slot_floor:
            self._slot_floor = confirmed
        self._record()

    def _record(self) -> None:
        if self._slot_floor <= self._recorded:
            return
        offset = self.base_offset()
        offset.lsn = self._slot_floor
        self.lifecycle.checkpoint(offset)
        self._recorded = self._slot_floor

    def _slot_row(self) -> tuple[Any, Any] | None:
        """The slot's confirmed LSN and the pid of the walsender holding it,
        or ``None`` when the slot does not exist."""
        if self._watcher is None:
            settings = dict(self._replication_connection_settings())
            settings.pop("connection_factory", None)
            self._watcher = psycopg2.connect(**settings)
            self._watcher.autocommit = True
        with self._watcher.cursor() as cursor:
            cursor.execute(
                "select confirmed_flush_lsn - '0/0'::pg_lsn, active_pid "
                "from pg_replication_slots where slot_name = %s",
                (self._replication_slot,),
            )
            return cursor.fetchone()

    def _close_watcher(self) -> None:
        if self._watcher is not None:
            try:
                self._watcher.close()
            except psycopg2.Error:  # pragma: no cover
                pass
            self._watcher = None

    # ------------------------------------------------------------------
    # Schema extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_raw_schema_v1(change: dict) -> tuple[tuple[str, str, str], ...]:
        names = change.get("columnnames")
        types = change.get("columntypes")
        if not names or not types:
            old_keys = change.get("oldkeys") or {}
            names = old_keys.get("keynames")
            types = old_keys.get("keytypes")
        if not names or not types:
            raise PostgresErrorCode.POSTGRES_15.exception(payload=change)
        return tuple((name, dtype, dtype) for name, dtype in zip(names, types))

    @staticmethod
    def _extract_raw_schema_v2(payload: dict) -> tuple[tuple[str, str, str], ...]:
        columns = payload.get("columns") or payload.get("identity")
        if not columns:
            raise PostgresErrorCode.POSTGRES_15.exception(payload=payload)
        return tuple(
            (column["name"], column["type"], column["type"]) for column in columns
        )

    # ------------------------------------------------------------------
    # Value coercion -- wal2json emits everything as JSON, so dates /
    # decimals / binaries come over as strings the schema knows the
    # target type for.
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_value(value: Any, pa_type: pa.DataType) -> Any:
        if value is None:
            return None
        if isinstance(pa_type, pa.Decimal128Type):
            if not isinstance(value, Decimal):
                return Decimal(str(value))
        elif pa.types.is_integer(pa_type):
            if not isinstance(value, int) or isinstance(value, bool):
                return int(value)
        elif pa.types.is_floating(pa_type):
            if not isinstance(value, float):
                return float(value)
        elif pa.types.is_boolean(pa_type):
            if not isinstance(value, bool):
                if isinstance(value, str):
                    return value.lower() in ("t", "true", "1", "yes")
                return bool(value)
        elif pa.types.is_date(pa_type):
            if isinstance(value, str):
                return date.fromisoformat(value)
        elif pa.types.is_timestamp(pa_type):
            if isinstance(value, str):
                return datetime.fromisoformat(value)
        elif pa.types.is_time(pa_type):
            if isinstance(value, str):
                return dt_time.fromisoformat(value)
        elif pa.types.is_binary(pa_type):
            if isinstance(value, str):
                return bytes.fromhex(value.removeprefix("\\x"))
        return value

    @classmethod
    def _coerce_values(cls, values: dict, columns: Iterable) -> None:
        column_types = {column.name: column.patype for column in columns}
        for column, value in list(values.items()):
            pa_type = column_types.get(column)
            if pa_type is None:
                continue
            values[column] = cls._coerce_value(value, pa_type)

    def _build_change(
        self,
        *,
        qualified: str,
        op: CdcOperation,
        xid: str,
        sequence: int,
        timestamp: datetime,
        schema_version: SchemaVersion,
        new_values: dict,
        old_values: dict,
        offset: LsnOffset | None,
        segment: int,
    ) -> CdcDataChange:
        columns = schema_version.columns
        null_row = {column.name: None for column in columns}
        if not new_values:
            new_values = dict(null_row)
        if not old_values:
            old_values = dict(null_row)
        self._coerce_values(new_values, columns)
        self._coerce_values(old_values, columns)
        return CdcDataChange(
            operation=op,
            transaction=xid,
            sequence=sequence,
            timestamp=timestamp,
            table=qualified,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
            segment=segment,
            ordinal=sequence,
            schema=schema_version,
        )

    # ------------------------------------------------------------------
    # Poll loop body
    # ------------------------------------------------------------------

    def bound_stream(self, end: CdcOffset) -> None:
        if not isinstance(end, LsnOffset):
            raise PostgresErrorCode.POSTGRES_16.exception(actual=type(end).__name__)
        self._bound = end.lsn
        self._bound_reached = False

    def stream_bound_reached(self) -> bool:
        return self._bound_reached

    def _past_bound(self, commit: int, message: Any) -> bool:
        """Whether a transaction committing at `commit` is past the bound:
        then its opening `message` is held for the next poll, which reads on
        unbounded, and the bound is reached."""
        if self._bound is None or commit <= self._bound:
            return False
        self._held.append(message)
        self._reach_bound()
        return True

    def _bound_passed_quietly(self) -> bool:
        """Whether the server has sent everything up to the bound with no
        transaction past it to say so: its keepalives report how far it
        has decoded."""
        if self._bound is None or self._cursor is None:
            return False
        if self._cursor.wal_end < self._bound:
            return False
        self._reach_bound()
        return True

    def _reach_bound(self) -> None:
        self._bound = None
        self._bound_reached = True

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        match self._wal2json_format_version:
            case 1:
                yield from self._poll_changes_v1(session)
            case 2:
                yield from self._poll_changes_v2(session)
            case _:
                raise PostgresErrorCode.POSTGRES_17.exception(
                    version=self._wal2json_format_version,
                )

    def _read_next_message(
        self,
        *,
        pending: bool,
        deadline: float,
        pending_timeout: float,
    ) -> Any | None:
        # ``pending`` replaces the deadline -- when a v2 BEGIN has
        # arrived but COMMIT hasn't, wait for the next message until
        # ``pending_timeout`` so we don't synthesize a torn transaction.
        if self._held:
            return self._held.pop(0)
        effective = pending_timeout if pending else deadline
        while True:
            remaining = effective - time.monotonic()
            if remaining <= 0:
                return None
            if self._cursor is None:  # pragma: no cover
                return None
            message = self._cursor.read_message()
            self._watch_slot()
            if message is not None:
                return message
            sleep = min(POLL_INTERVAL_SECONDS, max(remaining, 0.0))
            if sleep <= 0:
                return None
            time.sleep(sleep)

    def _resolve_table(self, change: dict) -> str | None:
        schema = change.get("schema")
        table = change.get("table")
        if schema is None or table is None:
            return None
        return self._as_qualified_table(schema, table)

    # --- v1 -----------------------------------------------------------

    def _poll_changes_v1(
        self,
        _session: Any,
    ) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._poll_max_secs
        while True:
            message = self._read_next_message(
                pending=False,
                deadline=deadline,
                pending_timeout=deadline,
            )
            if message is None:
                self._confirm_idle()
                self._bound_passed_quietly()
                break
            if self._past_bound(message.data_start, message):
                break
            payload = orjson.loads(message.payload)
            xid = str(payload.get("xid", ""))
            timestamp = datetime.fromisoformat(payload["timestamp"]).astimezone(
                timezone.utc
            )
            changes = payload.get("change", [])
            lsn_offset = LsnOffset(lsn=message.data_start)
            sequence = 0
            for change in changes:
                sequence += 1
                produced = self._process_change_v1(
                    change=change,
                    xid=xid,
                    sequence=sequence,
                    timestamp=timestamp,
                    offset=lsn_offset,
                )
                if produced is not None:
                    self._unacknowledged += 1
                    yield produced
            self._mark = lsn_offset.copy()

    def _process_change_v1(
        self,
        *,
        change: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset,
    ) -> CdcDataChange | None:
        qualified = self._resolve_table(change)
        if qualified is None:
            return None
        if self._schema_registry is None:  # pragma: no cover
            return None
        raw_schema = self._extract_raw_schema_v1(change)
        schema_version = self._schema_registry.register_from_log(
            qualified,
            raw_schema,
        )
        kind = change.get("kind")
        op = _V1_OPERATION_MAP.get(kind)
        if op is None:
            return None
        if op is CdcOperation.INSERT:
            new_values = self._extract_new_values_v1(change)
            old_values = {column: None for column in new_values}
        elif op is CdcOperation.UPDATE:
            new_values = self._extract_new_values_v1(change)
            old_values = self._extract_old_values_v1(change)
        else:  # DELETE
            old_values_raw = self._extract_old_values_v1(change)
            new_values_raw = self._extract_new_values_v1(change)
            new_values = old_values_raw or new_values_raw or {}
            old_values = {column: None for column in new_values}
        return self._build_change(
            qualified=qualified,
            op=op,
            xid=xid,
            sequence=sequence,
            timestamp=timestamp,
            schema_version=schema_version,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
            segment=offset.lsn,
        )

    @staticmethod
    def _extract_new_values_v1(change: dict) -> dict:
        columns = change.get("columnnames")
        values = change.get("columnvalues")
        if columns and values:
            return dict(zip(columns, values))
        return {}

    @staticmethod
    def _extract_old_values_v1(change: dict) -> dict:
        old_values = change.get("oldkeys") or {}
        columns = old_values.get("keynames")
        values = old_values.get("keyvalues")
        if columns and values:
            return dict(zip(columns, values))
        return {}

    # --- v2 -----------------------------------------------------------

    def _poll_changes_v2(
        self,
        _session: Any,
    ) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._poll_max_secs
        idle_budget = self._poll_max_secs * PENDING_TIMEOUT_MULTIPLIER
        xid: str | None = None
        timestamp: datetime | None = None
        commit: int = 0
        sequence: int = 0
        while True:
            pending = xid is not None
            message = self._read_next_message(
                pending=pending,
                deadline=deadline,
                pending_timeout=time.monotonic() + idle_budget,
            )
            if message is None:
                if pending:
                    raise PostgresErrorCode.POSTGRES_18.exception(xid=xid)
                self._confirm_idle()
                self._bound_passed_quietly()
                break
            payload = orjson.loads(message.payload)
            action = payload.get("action")
            if action == _V2Actions.BEGIN:
                xid = str(payload.get("xid", ""))
                commit = self._commit_lsn(payload, xid)
                if self._past_bound(commit, message):
                    break
                timestamp = datetime.fromisoformat(payload["timestamp"]).astimezone(
                    timezone.utc
                )
                sequence = 0
                continue
            if action == _V2Actions.COMMIT:
                self._mark = LsnOffset(lsn=message.wal_end)
                xid = None
                timestamp = None
                if self._bound is not None and commit == self._bound:
                    self._reach_bound()
                    break
                continue
            if timestamp is None:
                raise PostgresErrorCode.POSTGRES_34.exception(
                    action=action, lsn=message.data_start
                )
            if action == _V2Actions.TRUNCATE:
                sequence += 1
                truncated = self._truncate_change(
                    payload=payload,
                    xid=xid or "",
                    sequence=sequence,
                    timestamp=timestamp,
                    offset=LsnOffset(lsn=message.data_start),
                    segment=commit,
                )
                if truncated is not None:
                    self._unacknowledged += 1
                    yield truncated
                continue
            lsn_offset = LsnOffset(lsn=message.data_start)
            sequence += 1
            produced = self._process_change_v2(
                payload=payload,
                xid=xid or "",
                sequence=sequence,
                timestamp=timestamp,
                offset=lsn_offset,
                segment=commit,
            )
            if produced is not None:
                self._unacknowledged += 1
                yield produced

    @staticmethod
    def _commit_lsn(payload: dict, xid: str) -> int:
        """The commit position of the transaction a format-2 BEGIN opens.

        Every row of the transaction takes it as its segment. A row's own
        position is where it was written, which can be long before its
        transaction commits: transactions arrive in commit order, so a later
        one can hold rows written before an earlier one's, and those rows
        would sort below what the stage publisher has already published and
        be dropped. Commit positions grow in the order transactions
        arrive."""
        lsn = payload.get("lsn")
        if not isinstance(lsn, str):
            raise PostgresErrorCode.POSTGRES_30.exception(xid=xid)
        return lsn_to_int(lsn)

    def _process_change_v2(
        self,
        *,
        payload: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset,
        segment: int,
    ) -> CdcDataChange | None:
        qualified = self._resolve_table(payload)
        if qualified is None:
            return None
        if self._schema_registry is None:  # pragma: no cover
            return None
        raw_schema = self._extract_raw_schema_v2(payload)
        schema_version = self._schema_registry.register_from_log(
            qualified,
            raw_schema,
        )
        action = payload.get("action")
        op = _V2_OPERATION_MAP.get(action)
        if op is None:
            return None
        if op is CdcOperation.INSERT:
            new_values = self._extract_new_values_v2(payload)
            old_values = {column: None for column in new_values}
        elif op is CdcOperation.UPDATE:
            new_values = self._extract_new_values_v2(payload)
            old_values = self._extract_old_values_v2(payload)
        else:  # DELETE
            old_values_raw = self._extract_old_values_v2(payload)
            new_values_raw = self._extract_new_values_v2(payload)
            new_values = old_values_raw or new_values_raw or {}
            old_values = {column: None for column in new_values}
        return self._build_change(
            qualified=qualified,
            op=op,
            xid=xid,
            sequence=sequence,
            timestamp=timestamp,
            schema_version=schema_version,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
            segment=segment,
        )

    def _truncate_change(
        self,
        *,
        payload: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset,
        segment: int,
    ) -> CdcDataChange | None:
        qualified = self._resolve_table(payload)
        if qualified is None or self._schema_registry is None:
            return None
        schema_version = (
            self._schema_registry.latest(qualified) or self._schema_registry.unknown()
        )
        null_row = {column.name: None for column in schema_version.columns}
        return CdcDataChange(
            operation=CdcOperation.TRUNCATE,
            transaction=xid,
            sequence=sequence,
            timestamp=timestamp,
            table=qualified,
            new_values=dict(null_row),
            old_values=dict(null_row),
            offset=offset,
            segment=segment,
            ordinal=sequence,
            schema=schema_version,
        )

    @staticmethod
    def _extract_new_values_v2(change: dict) -> dict:
        columns = change.get("columns")
        if not columns:
            return {}
        return {column["name"]: column["value"] for column in columns}

    @staticmethod
    def _extract_old_values_v2(change: dict) -> dict:
        identity = change.get("identity")
        if not identity:
            return {}
        return {column["name"]: column["value"] for column in identity}
