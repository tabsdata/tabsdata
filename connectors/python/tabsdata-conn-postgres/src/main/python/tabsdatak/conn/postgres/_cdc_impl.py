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
  large-column detection, and wal2json-type-string ->
  SQLAlchemy mapping (``ddl_to_sa`` over the type strings wal2json
  emits in each change row).
* :class:`Wal2JsonIngester` (+ :class:`LsnOffset`) -- the
  ``CdcIngester`` instance that drives a Postgres logical
  replication slot via ``psycopg2``'s
  ``LogicalReplicationConnection`` and emits ``CdcDataChange``
  records for the runner. Supports wal2json output-plugin format
  versions 1 and 2.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator, Iterable
from datetime import date, datetime, time as dt_time, timezone, tzinfo
from decimal import Decimal
from typing import Any, Literal, TypeAlias, Union

import orjson
import psycopg2
import pyarrow as pa
import sqlalchemy.types as sa_types
from psycopg2.extras import LogicalReplicationConnection
from sqlalchemy import URL, create_engine, make_url, text
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import BLOB, CLOB, LargeBinary, Text

from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._ingester import CdcIngester
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DOT,
    CdcOperation,
)
from tabsdatak.conn.common.sql.cdc.dialect import CdcIngesterType, Dialect
from tabsdatak.conn.common.sql.cdc.typing import QualifiedTablesSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.postgres import LsnPosition
from tabsdatak.conn.postgres.error import PostgresErrorCode
from tabsdatak.spi import CommitStateSpec

logger = logging.getLogger(__name__)


# --- Postgres-specific runtime constants ---------------------------

# wal2json's logical-decoding output plugin.
WAL2JSON_PLUGIN = "wal2json"

DEFAULT_REPLICATION_SLOT_BEHAVIOR = "reuse"

# Loop cadence inside the wal2json reader when the cursor has no
# message available -- cheap sleeps so the runner can interrupt.
POLL_INTERVAL_SECONDS = 1.0

# When a transaction has begun (v2 BEGIN seen) but no COMMIT has
# arrived yet, extend the per-poll deadline by this multiplier so
# we don't synthesize a half-applied transaction into the staging
# area. Mirrors the legacy ``Wal2JsonIngester``.
PENDING_TIMEOUT_MULTIPLIER = 16

DEFAULT_WAL2JSON_FORMAT_VERSION: int = 2


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

# Columns whose contents we drop from CDC ingestion (LOB family).
_LARGE_COLUMN_TYPES = (
    BLOB,
    CLOB,
    LargeBinary,
    Text,
    BYTEA,
)


def _parse_offset_string(value: str) -> tzinfo | None:
    """Parse ``"+05:30"`` / ``"-08:00"`` style offsets into a
    ``timezone``. Returns ``None`` on unparseable input."""
    from datetime import timedelta, timezone

    value = value.strip()
    if not value or value[0] not in "+-":
        return None
    sign = 1 if value[0] == "+" else -1
    try:
        hh, _, mm = value[1:].partition(":")
        hours = int(hh)
        minutes = int(mm or 0)
    except ValueError:
        return None
    delta = timedelta(hours=hours, minutes=minutes) * sign
    return timezone(delta)


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

    def parse_qualified_table(self, tablespec: str) -> tuple[quoted_name, quoted_name]:
        spec = tablespec.strip()
        if not spec or "." not in spec:
            raise ValueError(f"expected 'schema.table', got {tablespec!r}")
        schema_part, _, table_part = spec.partition(".")

        def _unquote(part: str) -> tuple[str, bool]:
            if part.startswith('"') and part.endswith('"') and len(part) >= 2:
                return part[1:-1].replace('""', '"'), True
            return part, False

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
            return _parse_offset_string(str(row[0]))
        return None

    def is_large_column(self, satype: Any) -> bool:
        return isinstance(satype, _LARGE_COLUMN_TYPES)

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


class LsnOffset(CdcOffset):
    """Postgres CDC offset = the WAL LSN the consumer has flushed
    so far. Zero is the cold-start marker (replay from the oldest
    WAL the slot retains)."""

    def __init__(self, lsn: int = 0):
        self.lsn = lsn

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, LsnOffset):
            raise PostgresErrorCode.POSTGRES_16.exception(
                actual=type(other).__name__,
            )
        self.lsn = other.lsn

    def write(self) -> dict:
        return {"lsn": self.lsn}

    @classmethod
    def read(cls, data: dict) -> "LsnOffset":
        return cls(lsn=int(data.get("lsn", 0)))

    def copy(self) -> "LsnOffset":
        return LsnOffset(lsn=self.lsn)


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
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        publication_name: str | None = None,
        wal2json_format_version: int = DEFAULT_WAL2JSON_FORMAT_VERSION,
    ):
        super().__init__(tables=tables)
        self._conn = conn
        self._start_from = start_from
        self._replication_slot = replication_slot
        self._replication_slot_behavior = replication_slot_behavior
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._publication_name = publication_name
        self._wal2json_format_version = wal2json_format_version
        self._mark: LsnOffset | None = None
        self._base_offset: LsnOffset | None = None
        self._replicator: Any | None = None
        self._cursor: Any | None = None

    @property
    def ingester_type(self) -> CdcIngesterType:
        return "wal2json"

    @property
    def dialect(self) -> PostgresDialect:
        return PostgresDialect()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(self) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("PostgreSQL server timezone: %s", self._server_tz)
        self.init_stream()
        return engine

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
        if session is not None:
            try:
                session.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose SA engine: %s", exc)

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _resolve_url(self) -> URL:
        url = make_url(_convert(self._conn.uri, str))
        creds = self._conn.credentials
        return url.set(
            username=creds.user.value(),
            password=creds.password.value(),
        )

    def _build_engine(self) -> Any:
        return create_engine(self._resolve_url())

    def _replication_connection_settings(self) -> dict[str, Any]:
        url = self._resolve_url()
        settings: dict[str, Any] = {
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

        options: dict[str, str] = {
            "format-version": str(self._wal2json_format_version),
            "include-transaction": "true",
            "include-xids": "true",
            "include-timestamp": "true",
            "add-tables": self._schema_table_filter(),
        }
        if self._publication_name:
            options["publication_names"] = self._publication_name

        self._base_offset = self._mark.copy()
        self._cursor.start_replication(
            slot_name=self._replication_slot,
            decode=True,
            start_lsn=self._mark.lsn,
            options=options,
        )
        logger.info("Replication slot %r attached", self._replication_slot)

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

    def commit_offset(self, offset: CdcOffset) -> None:
        if not isinstance(offset, LsnOffset):
            raise PostgresErrorCode.POSTGRES_16.exception(
                actual=type(offset).__name__,
            )
        if self._cursor is None:  # pragma: no cover -- stream init failed
            return
        # Postgres' standby-status update tells the slot it can
        # garbage-collect WAL up to the flushed position. Use the
        # current ``_mark`` (last fully-applied commit) when set.
        flush_lsn = self._mark.lsn if self._mark is not None else offset.lsn
        self._cursor.send_feedback(flush_lsn=flush_lsn, reply=True)
        logger.debug("Confirmed LSN position %d", flush_lsn)

    # Postgres keeps the offset in its replication slot (external-owned),
    # so it overrides the Tabsdata-owned defaults: commit by advancing the
    # slot, and let the slot resume itself rather than reading Tabsdata
    # ``stream_state``.

    def commit_state(self, offset: CdcOffset) -> CommitStateSpec:
        return lambda: self.commit_offset(offset)

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> CdcOffset | None:
        # The slot resumes from its server-side confirmed position; the
        # cold-start mark is seeded from ``start_from`` by ``init_stream``.
        return None

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
    ) -> CdcDataChange:
        columns = schema_version.columns
        null_row = {column.name: None for column in columns}
        if not new_values:
            new_values = dict(null_row)
        if not old_values:
            old_values = dict(null_row)
        self._coerce_values(new_values, columns)
        self._coerce_values(old_values, columns)
        self.localize_values(new_values)
        self.localize_values(old_values)
        return CdcDataChange(
            operation=op,
            transaction=xid,
            sequence=sequence,
            timestamp=timestamp,
            table=qualified,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
            schema=schema_version,
        )

    # ------------------------------------------------------------------
    # Poll loop body
    # ------------------------------------------------------------------

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
        # ``pending`` extends the deadline -- when a v2 BEGIN has
        # arrived but COMMIT hasn't, give the source more time so
        # we don't synthesize a torn transaction.
        effective = pending_timeout if pending else deadline
        while True:
            remaining = effective - time.monotonic()
            if remaining <= 0:
                return None
            if self._cursor is None:  # pragma: no cover
                return None
            message = self._cursor.read_message()
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
        deadline = time.monotonic() + self._blocking_timeout_seconds
        while True:
            message = self._read_next_message(
                pending=False,
                deadline=deadline,
                pending_timeout=deadline,
            )
            if message is None:
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
        deadline = time.monotonic() + self._blocking_timeout_seconds
        pending_timeout = time.monotonic() + (
            self._blocking_timeout_seconds * PENDING_TIMEOUT_MULTIPLIER
        )
        xid: str | None = None
        timestamp: datetime | None = None
        sequence: int = 0
        while True:
            pending = xid is not None
            message = self._read_next_message(
                pending=pending,
                deadline=deadline,
                pending_timeout=pending_timeout,
            )
            if message is None:
                if pending:
                    raise PostgresErrorCode.POSTGRES_18.exception(xid=xid)
                break
            payload = orjson.loads(message.payload)
            action = payload.get("action")
            if action == _V2Actions.BEGIN:
                xid = str(payload.get("xid", ""))
                timestamp = datetime.fromisoformat(payload["timestamp"]).astimezone(
                    timezone.utc
                )
                sequence = 0
                continue
            if action == _V2Actions.COMMIT:
                self._mark = LsnOffset(lsn=message.wal_end)
                xid = None
                timestamp = None
                continue
            if action == _V2Actions.TRUNCATE:
                continue
            lsn_offset = LsnOffset(lsn=message.data_start)
            sequence += 1
            produced = self._process_change_v2(
                payload=payload,
                xid=xid or "",
                sequence=sequence,
                timestamp=timestamp,
                offset=lsn_offset,
            )
            if produced is not None:
                yield produced

    def _process_change_v2(
        self,
        *,
        payload: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset,
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
