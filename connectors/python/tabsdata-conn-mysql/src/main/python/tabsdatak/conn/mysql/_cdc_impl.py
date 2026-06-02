"""MySQL CDC implementation -- dialect + binlog ingester.

Mirrors the layering of the batch side (``_impl.py``): the connector
front (``MySQLCdcStreamSrcConn`` / ``MySQLCdcStreamSrc`` in ``__init__.py``)
and the plugin glue (``_cdc_plugin.py``) sit above this module;
this is where the pymysqlreplication + sqlalchemy machinery lives.

Two pieces:

* :class:`MySQLDialect` -- implements the dialect-agnostic
  ``Dialect`` abstract for catalog lookups, identifier quoting,
  large-column detection, and binlog-field-type -> SQLAlchemy
  mapping.
* :class:`BinlogIngester` (+ :class:`GtidOffset`) -- the
  ``CdcIngester`` instance that reads MySQL binlog events via
  ``pymysqlreplication`` and emits ``CdcDataChange`` records for
  the runner.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Literal, TypeAlias, Union

import pymysql
import pymysqlreplication.constants.FIELD_TYPE as FIELD_TYPE
import sqlalchemy.types as sa_types
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
    GtidEvent,
    HeartbeatLogEvent,
    QueryEvent,
    XidEvent,
)
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from sqlalchemy import URL, create_engine, make_url, text
from sqlalchemy.dialects.mysql import (
    BIT,
    DATETIME,
    DOUBLE,
    ENUM,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMINT,
    MEDIUMTEXT,
    SET,
    TIME,
    TIMESTAMP,
    TINYBLOB,
    TINYINT,
    YEAR,
)
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import BLOB, CLOB, LargeBinary, Text

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
from tabsdatak.conn.common.sql.cdc.typing import (
    QualifiedTablesSpec,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.mysql import BinlogPosition, GtidPosition
from tabsdatak.conn.mysql.error import MysqlErrorCode

logger = logging.getLogger(__name__)


# --- Mysql-specific runtime constants -------------------------------

# Internal: legacy uses a 1s slave heartbeat. Capped against the
# stream's blocking timeout.
HEARTBEAT_INTERVAL_SECONDS = 1

# pymysqlreplication's binlog file's start byte (after the magic
# bytes). Used for ``RelativePositionSpec="head"``.
BINLOG_START_POSITION = 4

# Placeholder timestamp stamped on rows held in the buffered poll's
# pending list. The real commit timestamp is written in on the next
# XidEvent / QueryEvent("COMMIT") that closes the transaction.
PENDING_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)

# When True the poll loop buffers rows per-transaction and stamps
# them with the commit timestamp on XidEvent / QueryEvent("COMMIT").
# When False each row carries its own binlog-event timestamp (the
# behaviour pre-TS port).
BUFFER_TRANSACTIONS = True


# Internal start-position shape consumed by the ingester after the
# plugin has converted the user-facing ``MySQLCdcStartFromSpec``.
# ``TimestampPosition`` here is the common runner type;
# ``GtidPosition`` / ``BinlogPosition`` are mysql-local (the
# ingester is mysql-only).
_InternalStartFromSpec: TypeAlias = Union[
    Literal["head", "tail"],
    GtidPosition,
    BinlogPosition,
    TimestampPosition,
]


# pymysqlreplication event-type -> CDC operation discriminator.
_OPERATION_MAP = {
    WriteRowsEvent: CdcOperation.INSERT,
    UpdateRowsEvent: CdcOperation.UPDATE,
    DeleteRowsEvent: CdcOperation.DELETE,
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
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    TINYBLOB,
)

# pymysqlreplication exposes binlog column types as integer codes;
# this map decodes them into SQLAlchemy types so the schema
# registry can build a PyArrow schema for downstream consumers.
_FIELD_TYPE_MAP: dict[int, type] = {
    FIELD_TYPE.TINY: TINYINT,
    FIELD_TYPE.SHORT: sa_types.SmallInteger,
    FIELD_TYPE.LONG: sa_types.Integer,
    FIELD_TYPE.FLOAT: sa_types.Float,
    FIELD_TYPE.DOUBLE: DOUBLE,
    FIELD_TYPE.NULL: sa_types.NullType,
    FIELD_TYPE.TIMESTAMP: TIMESTAMP,
    FIELD_TYPE.LONGLONG: sa_types.BigInteger,
    FIELD_TYPE.INT24: MEDIUMINT,
    FIELD_TYPE.DATE: sa_types.Date,
    FIELD_TYPE.TIME: TIME,
    FIELD_TYPE.DATETIME: DATETIME,
    FIELD_TYPE.YEAR: YEAR,
    FIELD_TYPE.VARCHAR: sa_types.String,
    FIELD_TYPE.BIT: BIT,
    FIELD_TYPE.JSON: sa_types.JSON,
    FIELD_TYPE.NEWDECIMAL: sa_types.Numeric,
    FIELD_TYPE.ENUM: ENUM,
    FIELD_TYPE.SET: SET,
    FIELD_TYPE.TINY_BLOB: TINYBLOB,
    FIELD_TYPE.MEDIUM_BLOB: MEDIUMBLOB,
    FIELD_TYPE.LONG_BLOB: LONGBLOB,
    FIELD_TYPE.BLOB: sa_types.LargeBinary,
    FIELD_TYPE.VAR_STRING: sa_types.String,
    FIELD_TYPE.STRING: sa_types.String,
    FIELD_TYPE.GEOMETRY: sa_types.LargeBinary,
}

# Some MySQL versions distinguish ``TIMESTAMP2`` / ``DATETIME2`` /
# ``TIME2`` (fractional-second variants). Alias them to the same
# SA types.
for _attr in ("TIMESTAMP2", "DATETIME2", "TIME2"):
    _code = getattr(FIELD_TYPE, _attr, None)
    if _code is not None:
        _base_attr = _attr.rstrip("2")
        _FIELD_TYPE_MAP[_code] = _FIELD_TYPE_MAP[getattr(FIELD_TYPE, _base_attr)]


def _parse_offset_string(value: str) -> tzinfo | None:
    """Parse ``"+05:30"`` / ``"-08:00"`` style offsets into a
    ``timezone``. Returns ``None`` on unparseable input."""
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


class MySQLDialect(Dialect):
    """``Dialect`` for the MySQL CDC ingester."""

    @property
    def open_quote(self) -> str:
        return "`"

    @property
    def close_quote(self) -> str:
        return "`"

    def default_case_fold(self, name: str) -> str:
        return name

    def parse_qualified_table(self, tablespec: str) -> tuple[quoted_name, quoted_name]:
        spec = tablespec.strip()
        if not spec or "." not in spec:
            raise ValueError(f"expected 'schema.table', got {tablespec!r}")
        schema_part, _, table_part = spec.partition(".")

        def _unquote(part: str) -> tuple[str, bool]:
            if part.startswith("`") and part.endswith("`") and len(part) >= 2:
                return part[1:-1], True
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
        # MySQL's session-vs-global tz can disagree; prefer global.
        # When set to "SYSTEM", compute the offset via
        # ``timediff(now(), utc_timestamp())``.
        with engine.connect() as connection:
            row = connection.execute(
                text("select @@global.time_zone, @@session.time_zone")
            ).fetchone()
        if not row or row[0] is None:
            return None
        tz_value = row[0]
        if tz_value == "SYSTEM":
            with engine.connect() as connection:
                row = connection.execute(
                    text("select timediff(now(), utc_timestamp())")
                ).fetchone()
            if row and row[0] is not None:
                td = row[0]
                if isinstance(td, timedelta):
                    return timezone(td)
                return _parse_offset_string(str(td))
            return None
        return _parse_offset_string(tz_value)

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
                "select column_name "
                "from information_schema.columns "
                "where table_schema = :schema "
                "and table_name = :table "
                "order by ordinal_position"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
        return [row[0] for row in rows]

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        if ingester_type == "binlog":
            return self._cdc_to_sa_binlog(cdc_dtype)
        raise NotImplementedError(
            f"{type(self).__name__}.cdc_to_sa not implemented for "
            f"ingester_type={ingester_type!r}"
        )

    @staticmethod
    def _cdc_to_sa_binlog(cdc_type: Any) -> sa_types.TypeEngine:
        column_type = getattr(cdc_type, "type", None)
        column_class = _FIELD_TYPE_MAP.get(column_type)
        if column_class is None:
            raise ValueError(
                f"Unsupported MySQL binlog cdc type: {cdc_type!r}"
                f" (code={column_type!r})"
            )
        if column_class is sa_types.Numeric:
            precision = getattr(cdc_type, "precision", None)
            decimals = getattr(cdc_type, "decimals", None)
            kw: dict[str, Any] = {}
            if precision is not None:
                kw["precision"] = precision
            if decimals is not None:
                kw["scale"] = decimals
            return sa_types.Numeric(**kw)
        if column_class is sa_types.String:
            max_length = getattr(cdc_type, "max_length", None)
            if max_length:
                return sa_types.String(length=max_length)
            return sa_types.String()
        if column_class in (DATETIME, TIME, TIMESTAMP):
            fsp = getattr(cdc_type, "fsp", None)
            if fsp is not None:
                return column_class(fsp=fsp)
            return column_class()
        if column_class is ENUM:
            enum_values = getattr(cdc_type, "enum_values", None)
            if enum_values:
                return ENUM(*enum_values)
            return ENUM()
        if column_class is SET:
            set_values = getattr(cdc_type, "set_values", None)
            if set_values:
                return SET(*set_values)
            return SET()
        if column_class is sa_types.Float:
            size = getattr(cdc_type, "size", None)
            if size:
                return sa_types.Float(precision=size)
            return sa_types.Float()
        return column_class()


# ---------------------------------------------------------------------
# Offset
# ---------------------------------------------------------------------


class GtidOffset(CdcOffset):
    """MySQL CDC offset = the GTID set the binlog stream has
    consumed so far. Empty string is the cold-start marker."""

    def __init__(self, gtid: str = ""):
        self.gtid = gtid

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, GtidOffset):
            raise MysqlErrorCode.MYSQL_17.exception(
                actual=type(other).__name__,
            )
        self.gtid = other.gtid

    def write(self) -> dict:
        return {"gtid": self.gtid}

    @classmethod
    def read(cls, data: dict) -> "GtidOffset":
        return cls(gtid=data.get("gtid", ""))

    def copy(self) -> "GtidOffset":
        return GtidOffset(gtid=self.gtid)


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class BinlogIngester(CdcIngester):
    """``CdcIngester`` for MySQL via ``pymysqlreplication``.

    Opens a SQLAlchemy engine (catalog introspection) plus a
    separate ``BinLogStreamReader`` (change stream). Polled by
    ``run_cdc``; emits ``CdcDataChange`` per row event."""

    def __init__(
        self,
        # The ``Conn`` is duck-typed: the ingester only reads
        # ``uri`` / ``credentials`` / ``conn_cfg``. Concrete type is
        # MySQLCdcStreamSrcConn but we don't import it here to avoid the
        # __init__.py -> _cdc_impl.py cycle.
        conn: Any,
        tables: QualifiedTablesSpec,
        start_from: _InternalStartFromSpec,
        server_id: int,
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    ):
        super().__init__(tables=tables)
        self._conn = conn
        self._start_from = start_from
        self._server_id = server_id
        self._blocking_timeout_seconds = blocking_timeout_seconds
        # ``_mark`` advances as GTID events flow past; ``_base_offset``
        # is the snapshot taken at stream init for the runner.
        self._mark: GtidOffset | None = None
        self._base_offset: GtidOffset | None = None
        self._stream: BinLogStreamReader | None = None

    @property
    def ingester_type(self) -> CdcIngesterType:
        return "binlog"

    @property
    def dialect(self) -> MySQLDialect:
        return MySQLDialect()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(self) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("MySQL server timezone: %s", self._server_tz)
        self.init_stream()
        return engine

    def close_session(self, session: Any) -> None:
        if self._stream is not None:
            try:
                self._stream.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close binlog stream: %s", exc)
            finally:
                self._stream = None
        if session is not None:
            try:
                session.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose SA engine: %s", exc)

    # ------------------------------------------------------------------
    # Connection helpers (URI parsing, engine + pymysqlreplication
    # settings derive from a single ``conn``)
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

    def _stream_connection_settings(self) -> dict[str, Any]:
        url = self._resolve_url()
        settings: dict[str, Any] = {
            "host": url.host,
            "port": url.port or 3306,
            "user": url.username,
            "passwd": url.password,
        }
        if url.database:
            settings["db"] = url.database
        settings["read_timeout"] = int(self._blocking_timeout_seconds)
        return settings

    # ------------------------------------------------------------------
    # Stream init -- translate start_from into pymysqlreplication kwargs
    # ------------------------------------------------------------------

    def init_stream(self) -> None:
        stream_kwargs: dict[str, Any] = {
            "connection_settings": self._stream_connection_settings(),
            "server_id": self._server_id,
            "blocking": True,
            "resume_stream": True,
            "slave_heartbeat": min(
                self._blocking_timeout_seconds, HEARTBEAT_INTERVAL_SECONDS
            ),
            "only_events": [
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
                GtidEvent,
                QueryEvent,
                XidEvent,
                HeartbeatLogEvent,
            ],
        }
        stream_schemas, stream_tables = self._schema_table_filter()
        if stream_schemas:
            stream_kwargs["only_schemas"] = stream_schemas
        if stream_tables:
            stream_kwargs["only_tables"] = stream_tables

        start_from = self._start_from
        # When restarting from a persisted offset, override the
        # user's ``start_from`` so we don't replay events.
        if self._mark is not None and self._mark.gtid:
            start_from = GtidPosition(gtid=self._mark.gtid)

        match start_from:
            case GtidPosition(gtid=gtid):
                stream_kwargs["auto_position"] = self._as_gtid_set(gtid)
                if self._mark is None:
                    self._mark = GtidOffset(gtid=gtid)
            case BinlogPosition(file=file, pos=pos):
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                if self._mark is None:
                    self._mark = GtidOffset()
            case TimestampPosition(ts=ts):
                stream_kwargs["skip_to_timestamp"] = ts
                if self._mark is None:
                    self._mark = GtidOffset()
            case "head":
                stream_kwargs["log_file"] = self._first_binlog_file()
                stream_kwargs["log_pos"] = BINLOG_START_POSITION
                self._mark = GtidOffset()
            case "tail":
                file, pos = self._last_binlog_position()
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                self._mark = GtidOffset()
            case _:
                raise MysqlErrorCode.MYSQL_14.exception(start_from=start_from)

        self._base_offset = self._mark.copy()
        self._stream = BinLogStreamReader(**stream_kwargs)
        logger.info("BinLogStreamReader initialised")

    @staticmethod
    def _as_gtid_set(gtid: str) -> str:
        # pymysqlreplication's ``auto_position`` wants a GTID *set*
        # (``uuid:1-N``), not a single position (``uuid:N``).
        if not gtid:
            return gtid
        uuid, _, seq = gtid.rpartition(":")
        if not uuid or "-" in seq:
            return gtid
        return f"{uuid}:1-{seq}"

    def _first_binlog_file(self) -> str:
        settings = self._stream_connection_settings()
        conn = pymysql.connect(
            host=settings["host"],
            port=settings["port"],
            user=settings.get("user"),
            passwd=settings.get("passwd"),
            db=settings.get("db"),
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("show binary logs")
                row = cursor.fetchone()
                if not row:
                    raise MysqlErrorCode.MYSQL_15.exception(position="head")
                return row[0]
        finally:
            conn.close()

    def _last_binlog_position(self) -> tuple[str, int]:
        settings = self._stream_connection_settings()
        conn = pymysql.connect(
            host=settings["host"],
            port=settings["port"],
            user=settings.get("user"),
            passwd=settings.get("passwd"),
            db=settings.get("db"),
        )
        try:
            with conn.cursor() as cursor:
                row = self._query_binlog_status(cursor)
                if not row:
                    raise MysqlErrorCode.MYSQL_15.exception(position="tail")
                return row[0], row[1]
        finally:
            conn.close()

    @staticmethod
    def _query_binlog_status(cursor: Any) -> Any:
        # MySQL 8.4+ renamed ``show master status`` to ``show
        # binary log status``. Try the new name first, fall back
        # to the legacy one.
        try:
            cursor.execute("show binary log status")
            return cursor.fetchone()
        except pymysql.err.ProgrammingError:
            cursor.execute("show master status")
            return cursor.fetchone()

    # ------------------------------------------------------------------
    # Table filter helpers
    # ------------------------------------------------------------------

    def _schema_table_filter(self) -> tuple[list[str] | None, list[str] | None]:
        # pymysqlreplication can pre-filter at the stream level when
        # both ``only_schemas`` and ``only_tables`` are set; this
        # spares it from yielding events the runner would discard.
        if not self._tracked_tables:
            return None, None
        schemas = set()
        tables = set()
        for tt in self._tracked_tables.values():
            schemas.add(tt.data_table.schema)
            tables.add(tt.data_table.table)
        return list(schemas), list(tables)

    def _as_qualified_table(self, schema: str, table: str) -> str | None:
        qualified = f"{schema}{DOT}{table}"
        if qualified in self._tracked_tables:
            return qualified
        return None

    # ------------------------------------------------------------------
    # Offset management
    # ------------------------------------------------------------------

    def base_offset(self) -> CdcOffset:
        # Always return a concrete GtidOffset (cold start -> empty).
        return (self._base_offset or GtidOffset()).copy()

    def commit_offset(self, offset: CdcOffset) -> None:
        # MySQL binlog has no acknowledgement protocol; the source
        # advances purely on stream-position. The runner still
        # invokes this so the dialect could record state if needed.
        if not isinstance(offset, GtidOffset):
            raise MysqlErrorCode.MYSQL_17.exception(
                actual=type(offset).__name__,
            )

    def offset_from_state(self, offset_state: dict) -> CdcOffset:
        # MySQL keeps no server-side consumer cursor: Tabsdata owns the
        # offset, so resume from the persisted GTID set.
        return GtidOffset.read(offset_state)

    # ------------------------------------------------------------------
    # Schema extraction
    # ------------------------------------------------------------------

    def _extract_raw_schema(self, event: Any) -> tuple[tuple[str, str, Any], ...]:
        # Build the per-event raw-schema tuple the registry de-dupes
        # on. Names come from the event's table-map; type-string
        # composes everything the type-mapper needs to round-trip
        # numeric precision / string length / enum values.
        schema = []
        for column in event.columns:
            name = column.name
            if not name:
                raise MysqlErrorCode.MYSQL_16.exception(table=event.table)
            parts = [str(column.type)]
            if getattr(column, "unsigned", False):
                parts.append("unsigned")
            if getattr(column, "max_length", None) is not None:
                parts.append(f"len={column.max_length}")
            if getattr(column, "precision", None) is not None:
                parts.append(f"prec={column.precision}")
            if getattr(column, "decimals", None) is not None:
                parts.append(f"dec={column.decimals}")
            if getattr(column, "fsp", None) is not None:
                parts.append(f"fsp={column.fsp}")
            schema.append((name, ":".join(parts), column))
        return tuple(schema)

    # ------------------------------------------------------------------
    # Poll loop body
    # ------------------------------------------------------------------

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        if BUFFER_TRANSACTIONS:
            yield from self._poll_changes_buffered(session)
        else:
            yield from self._poll_changes_unbuffered(session)

    def _poll_changes_unbuffered(
        self,
        session: Any,
    ) -> Generator[CdcChange, None, None]:
        # ``deadline`` caps the time we'll spend in a single poll
        # before returning control to the runner. Without it,
        # blocking=True + a stream that's idle would hang the
        # runner forever.
        deadline = time.monotonic() + self._blocking_timeout_seconds
        gtid: str | None = None
        seq: int = 0
        if self._stream is None:
            return
        for event in self._stream:
            if isinstance(event, GtidEvent):
                if gtid is not None:
                    # Advance the mark to the last completed GTID
                    # before adopting the new one.
                    self._mark = GtidOffset(gtid=gtid)
                if time.monotonic() > deadline:
                    return
                gtid = event.gtid
                seq = 0
            elif isinstance(event, HeartbeatLogEvent):
                if time.monotonic() > deadline:
                    return
            elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                qualified = self._as_qualified_table(event.schema, event.table)
                if qualified is None:
                    continue
                op = _OPERATION_MAP[type(event)]
                commit_ts = datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                raw_schema = self._extract_raw_schema(event)
                if self._schema_registry is None:  # pragma: no cover
                    return
                schema_version = self._schema_registry.register_from_log(
                    qualified,
                    raw_schema,
                )
                for row in event.rows:
                    seq += 1
                    match event:
                        case WriteRowsEvent():
                            new_values = row["values"]
                            old_values = {column: None for column in new_values}
                        case UpdateRowsEvent():
                            new_values = row["after_values"]
                            old_values = row["before_values"]
                        case DeleteRowsEvent():
                            new_values = row["values"]
                            old_values = {column: None for column in new_values}
                        case _:  # pragma: no cover - unreachable
                            continue
                    self.localize_values(new_values)
                    self.localize_values(old_values)
                    yield CdcDataChange(
                        operation=op,
                        transaction=gtid or "",
                        sequence=seq,
                        timestamp=commit_ts,
                        table=qualified,
                        new_values=new_values,
                        old_values=old_values,
                        offset=GtidOffset(gtid=gtid or ""),
                        schema=schema_version,
                    )

    def _poll_changes_buffered(
        self,
        session: Any,
    ) -> Generator[CdcChange, None, None]:
        # Buffered mode holds rows in ``pending`` until the closing
        # XidEvent / QueryEvent("COMMIT") arrives, so every yielded
        # row carries the *commit* timestamp (not the row's own
        # binlog-event timestamp).
        deadline = time.monotonic() + self._blocking_timeout_seconds
        gtid: str | None = None
        seq: int = 0
        pending: list[CdcDataChange] = []
        if self._stream is None:
            return
        for event in self._stream:
            if isinstance(event, GtidEvent):
                if not pending and time.monotonic() > deadline:
                    return
                gtid = event.gtid
                seq = 0
            elif isinstance(event, (XidEvent, QueryEvent)):
                if (
                    isinstance(event, QueryEvent)
                    and (event.query or "").strip().upper() != "COMMIT"
                ):
                    continue
                commit_timestamp = datetime.fromtimestamp(
                    event.timestamp,
                    tz=timezone.utc,
                )
                for change in pending:
                    change.timestamp = commit_timestamp
                    yield change
                if gtid is not None:
                    self._mark = GtidOffset(gtid=gtid)
                pending = []
                gtid = None
                seq = 0
            elif isinstance(event, HeartbeatLogEvent):
                if not pending and time.monotonic() > deadline:
                    return
            elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                qualified = self._as_qualified_table(event.schema, event.table)
                if qualified is None:
                    continue
                op = _OPERATION_MAP[type(event)]
                raw_schema = self._extract_raw_schema(event)
                if self._schema_registry is None:  # pragma: no cover
                    return
                schema_version = self._schema_registry.register_from_log(
                    qualified,
                    raw_schema,
                )
                for row in event.rows:
                    seq += 1
                    match event:
                        case WriteRowsEvent():
                            new_values = row["values"]
                            old_values = {column: None for column in new_values}
                        case UpdateRowsEvent():
                            new_values = row["after_values"]
                            old_values = row["before_values"]
                        case DeleteRowsEvent():
                            new_values = row["values"]
                            old_values = {column: None for column in new_values}
                        case _:  # pragma: no cover - unreachable
                            continue
                    self.localize_values(new_values)
                    self.localize_values(old_values)
                    pending.append(
                        CdcDataChange(
                            operation=op,
                            transaction=gtid or "",
                            sequence=seq,
                            timestamp=PENDING_TIMESTAMP,
                            table=qualified,
                            new_values=new_values,
                            old_values=old_values,
                            offset=GtidOffset(gtid=gtid or ""),
                            schema=schema_version,
                        )
                    )
