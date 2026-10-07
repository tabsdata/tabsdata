#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL CDC implementation -- dialect + binlog ingester.

Mirrors the layering of the batch side (``_impl.py``): the connector
front (``MySQLCdcStreamSrcConn`` / ``MySQLCdcStreamSrc`` in ``__init__.py``)
and the plugin glue (``_cdc_plugin.py``) sit above this module;
this is where the pymysqlreplication + sqlalchemy machinery lives.

Two pieces:

* :class:`MySQLDialect` -- implements the dialect-agnostic
  ``Dialect`` abstract for catalog lookups, identifier quoting,
  large-column classification, and binlog-field-type -> SQLAlchemy
  mapping.
* :class:`BinlogIngester` (+ :class:`GtidOffset`) -- the
  ``CdcIngester`` instance that reads MySQL binlog events via
  ``pymysqlreplication`` and emits ``CdcDataChange`` records for
  the runner.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Generator, Iterator
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Literal, TypeAlias, Union

import pymysql
import pymysqlreplication.constants.FIELD_TYPE as FIELD_TYPE
import sqlalchemy.types as sa_types
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
    FormatDescriptionEvent,
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
from sqlalchemy import URL, make_url, text
from sqlalchemy.dialects.mysql import (
    BIGINT,
    BINARY,
    BIT,
    DATETIME,
    DOUBLE,
    ENUM,
    FLOAT,
    INTEGER,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMINT,
    MEDIUMTEXT,
    SET,
    SMALLINT,
    TIME,
    TIMESTAMP,
    TINYBLOB,
    TINYINT,
    TINYTEXT,
    VARBINARY,
    YEAR,
)
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import LargeBinary, Text

from tabsdatak.conn.common._stream._offsets import TabsdataKeptOffsets

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import DEFAULT_POLL_MAX_SECS

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils
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
from tabsdatak.conn.common.sql.cdc.typing import (
    QualifiedTablesSpec,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.mysql import BinlogPosition, GtidPosition
from tabsdatak.conn.mysql._uri import normalize_mysql_cdc_uri
from tabsdatak.conn.mysql.error import MysqlErrorCode

logger = logging.getLogger(__name__)


# --- Mysql-specific runtime constants -------------------------------

# Internal: legacy uses a 1s slave heartbeat. Capped against the
# stream's blocking timeout.
HEARTBEAT_INTERVAL_SECONDS = 1

# pymysqlreplication's binlog file's start byte (after the magic
# bytes). Used for ``RelativePositionSpec="head"``.
BINLOG_START_POSITION = 4

# The largest lock_wait_timeout MySQL accepts (one year). The initial load's
# global read lock waits behind running statements, and giving up would fail
# the load, so it waits as long as the server allows.
INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS = 31536000

# Where, in the body of the GTID event that opens a transaction, MySQL
# 8.0.1 and later write the instant it was committed: after the flags, the
# source UUID, the transaction number and the logical clock, as microseconds
# since the epoch in seven bytes whose top bit says whether the original
# commit instant of a replicated transaction follows.
GTID_COMMIT_TIMESTAMP_OFFSET = 42
GTID_COMMIT_TIMESTAMP_BYTES = 7
GTID_COMMIT_TIMESTAMP_MASK = (1 << 55) - 1
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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

_LARGE_OBJECT_TYPES = (LargeBinary, TINYBLOB, MEDIUMBLOB, LONGBLOB)
_LARGE_TEXT_TYPES = (Text, TINYTEXT, MEDIUMTEXT, LONGTEXT, sa_types.JSON)

# The binlog reports all BLOB and TEXT variants under the single field
# type code FIELD_TYPE.BLOB. The table map tells them apart through two
# metadata items: ``length_size``, the width in bytes of the length prefix
# stored in front of each value, which fixes the size variant (1 -> TINY,
# 2 -> plain, 3 -> MEDIUM, 4 -> LONG); and the column charset, which
# separates the binary column (charset "binary" -> BLOB family) from the
# character one (any other charset -> TEXT family).
_BLOB_TYPES_BY_LENGTH_PREFIX_BYTES: dict[int, tuple[type, type]] = {
    1: (TINYBLOB, TINYTEXT),
    2: (LargeBinary, Text),
    3: (MEDIUMBLOB, MEDIUMTEXT),
    4: (LONGBLOB, LONGTEXT),
}
_BINARY_CHARSET = "binary"

_TRUNCATE_STATEMENT = re.compile(
    r"^truncate\s+(?:table\s+)?"
    r"(?:(?P<schema>`[^`]+`|\w+)\s*\.\s*)?"
    r"(?P<table>`[^`]+`|\w+)\s*;?$",
    re.IGNORECASE,
)


def _unquote_identifier(name: str) -> str:
    name = name.strip()
    if name.startswith("`") and name.endswith("`") and len(name) >= 2:
        return name[1:-1].replace("``", "`")
    return name


# pymysqlreplication exposes binlog column types as integer codes;
# this map decodes them into SQLAlchemy types so the schema
# registry can build a PyArrow schema for downstream consumers.
_FIELD_TYPE_MAP: dict[int, type] = {
    FIELD_TYPE.TINY: TINYINT,
    FIELD_TYPE.SHORT: SMALLINT,
    FIELD_TYPE.LONG: INTEGER,
    FIELD_TYPE.FLOAT: FLOAT,
    FIELD_TYPE.DOUBLE: DOUBLE,
    FIELD_TYPE.NULL: sa_types.NullType,
    FIELD_TYPE.TIMESTAMP: TIMESTAMP,
    FIELD_TYPE.LONGLONG: BIGINT,
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

_INTEGER_TYPES = (TINYINT, SMALLINT, MEDIUMINT, INTEGER, BIGINT)

# Some MySQL versions distinguish ``TIMESTAMP2`` / ``DATETIME2`` /
# ``TIME2`` (fractional-second variants). Alias them to the same
# SA types.
_TIMESTAMP_CODES = frozenset(
    code
    for code in (FIELD_TYPE.TIMESTAMP, getattr(FIELD_TYPE, "TIMESTAMP2", None))
    if code is not None
)

for _attr in ("TIMESTAMP2", "DATETIME2", "TIME2"):
    _code = getattr(FIELD_TYPE, _attr, None)
    if _code is not None:
        _base_attr = _attr.rstrip("2")
        _FIELD_TYPE_MAP[_code] = _FIELD_TYPE_MAP[getattr(FIELD_TYPE, _base_attr)]


def epoch_seconds(ts: datetime) -> int:
    aware = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    return int(aware.timestamp())


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

    def parse_qualified_table(
        self, tablespec: str
    ) -> tuple[quoted_name | None, quoted_name]:
        spec = tablespec.strip()
        if not spec:
            raise ValueError(f"expected 'schema.table' or 'table', got {tablespec!r}")

        def _unquote(part: str) -> tuple[str, bool]:
            if part.startswith("`") and part.endswith("`") and len(part) >= 2:
                return part[1:-1], True
            return part, False

        # Unqualified: the schema is filled from the connection's default
        # database by the caller (MySQL resolves unqualified names the same way).
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
                return parse_timezone(td)
            return None
        return parse_timezone(tz_value)

    def large_column_kind(self, satype: Any) -> LargeColumnKind | None:
        if isinstance(satype, _LARGE_OBJECT_TYPES):
            return "object"
        if isinstance(satype, _LARGE_TEXT_TYPES):
            return "text"
        return None

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
    def _blob_to_sa(cdc_type: Any) -> sa_types.TypeEngine:
        length_prefix_bytes = getattr(cdc_type, "length_size", 2)
        binary_class, text_class = _BLOB_TYPES_BY_LENGTH_PREFIX_BYTES[
            length_prefix_bytes
        ]
        charset = getattr(cdc_type, "character_set_name", None)
        if charset is None or charset.lower() == _BINARY_CHARSET:
            return binary_class()
        return text_class()

    @staticmethod
    def _cdc_to_sa_binlog(cdc_type: Any) -> sa_types.TypeEngine:
        column_type = getattr(cdc_type, "type", None)
        if column_type == FIELD_TYPE.BLOB:
            return MySQLDialect._blob_to_sa(cdc_type)
        column_class = _FIELD_TYPE_MAP.get(column_type)
        if column_class is None:
            raise ValueError(
                f"Unsupported MySQL binlog cdc type: {cdc_type!r}"
                f" (code={column_type!r})"
            )
        if column_class in _INTEGER_TYPES:
            # The catalog reflects the same flag, so both describe the column
            # alike; an unsigned one needs a wider type for its upper half.
            return column_class(unsigned=bool(getattr(cdc_type, "unsigned", False)))
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
            max_length = getattr(cdc_type, "max_length", None) or None
            charset = getattr(cdc_type, "character_set_name", None)
            if charset is not None and charset.lower() == _BINARY_CHARSET:
                if column_type == FIELD_TYPE.STRING:
                    return BINARY(length=max_length)
                return VARBINARY(length=max_length)
            return sa_types.String(length=max_length)
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
        return column_class()


# ---------------------------------------------------------------------
# Offset
# ---------------------------------------------------------------------


class BinlogInitialLoadSession(InitialLoadSession):
    """Snapshot pinned to a binlog position by holding the global read lock
    just long enough to start a repeatable-read transaction and read the
    executed GTID set. Writers are blocked only for those two statements,
    not while the tables are scanned.

    Reading the position without the lock would let a transaction commit
    between the snapshot forming and the position being read, and those
    changes would be in neither the snapshot nor the resumed stream.
    """

    def __init__(self, *, connection_settings: dict[str, Any]):
        super().__init__()
        self._connection = pymysql.connect(**connection_settings)
        self._require_gtid_mode()
        # Every row the load reads must come from the transaction opened
        # below, so the tables are read through an engine wrapping that one
        # connection instead of opening its own, which would see current
        # data rather than the snapshot.
        self._snapshot_engine = sqlalchemy_utils.create_engine(
            "mysql+pymysql://",
            creator=lambda: self._connection,
            poolclass=StaticPool,
        )
        # The engine's first connection initialises the dialect and then rolls
        # back, which would end the snapshot transaction, so it runs first.
        self._snapshot_engine.connect().close()
        # The catalog is read on connections of its own: every connection
        # handed back to its pool is rolled back, which on the snapshot's one
        # would end the snapshot before the tables are read, and they would
        # then be read at a later point than the position the stream resumes
        # from, delivering the changes in between twice.
        self._engine = sqlalchemy_utils.create_engine(
            "mysql+pymysql://",
            creator=lambda: pymysql.connect(**connection_settings),
        )
        cursor = self._connection.cursor()
        # A consistent snapshot is only consistent under repeatable read; under
        # any other level the server silently ignores the request.
        cursor.execute("set session transaction isolation level repeatable read")
        cursor.execute(
            f"set session lock_wait_timeout = {INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS}"
        )
        try:
            cursor.execute("flush tables with read lock")
        except pymysql.Error as error:
            self.close()
            raise MysqlErrorCode.MYSQL_21.exception(cause=error, reason=str(error))
        try:
            cursor.execute("start transaction with consistent snapshot, read only")
            cursor.execute("select @@global.gtid_executed")
            gtid = cursor.fetchone()[0] or ""
        finally:
            cursor.execute("unlock tables")
        cursor.execute("select utc_timestamp(6)")
        self._timestamp = cursor.fetchone()[0].replace(tzinfo=timezone.utc)
        self._offset = GtidOffset(gtid=gtid)
        logger.info("Initial load snapshot is consistent at GTID set %r", gtid)

    def _require_gtid_mode(self) -> None:
        cursor = self._connection.cursor()
        cursor.execute("select @@gtid_mode")
        mode = cursor.fetchone()[0]
        if str(mode).upper() != "ON":
            self._connection.close()
            raise MysqlErrorCode.MYSQL_20.exception(mode=mode)

    @property
    def engine(self) -> Any:
        return self._engine

    @property
    def offset(self) -> CdcOffset:
        return self._offset

    def tables(
        self, tracked_tables: dict[str, TrackedTable]
    ) -> Iterator[tuple[TrackedTable, Any]]:
        with self._snapshot_engine.connect() as connection:
            for tracked_table in tracked_tables.values():
                yield tracked_table, connection

    def close(self) -> None:
        # Disposing the snapshot engine closes the connection it was built
        # over, so the connection is only closed here when no engine took it.
        catalog, engine, connection = (
            self._engine,
            self._snapshot_engine,
            self._connection,
        )
        self._engine = self._snapshot_engine = self._connection = None
        if catalog is not None:
            try:
                catalog.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose initial-load catalog engine: %s", exc)
        if engine is not None:
            try:
                engine.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose initial-load engine: %s", exc)
        elif connection is not None:
            try:
                connection.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close initial-load connection: %s", exc)


_GTID_INTERVAL = re.compile(r"^(\d+)(?:-(\d+))?$")


def _opens_transaction(event: Any) -> bool:
    return (
        isinstance(event, QueryEvent) and (event.query or "").strip().upper() == "BEGIN"
    )


def _commits_transaction(event: Any) -> bool:
    return isinstance(event, XidEvent) or (
        (event.query or "").strip().upper() == "COMMIT"
    )


def _commit_timestamp(event: Any) -> datetime | None:
    """The instant the transaction a GTID event opens was committed, which
    the binlog holds ahead of the transaction's rows; ``None`` on a server
    older than 8.0.1, whose GTID events end at the logical clock."""
    end = GTID_COMMIT_TIMESTAMP_OFFSET + GTID_COMMIT_TIMESTAMP_BYTES
    if event.event_size < end:
        return None
    packet = event.packet
    skipped = GTID_COMMIT_TIMESTAMP_OFFSET - packet.read_bytes
    if skipped:
        packet.read(skipped)
    micros = int.from_bytes(packet.read(GTID_COMMIT_TIMESTAMP_BYTES), "little")
    return EPOCH + timedelta(microseconds=micros & GTID_COMMIT_TIMESTAMP_MASK)


class GtidSet:
    """A MySQL GTID set: for each source -- a server UUID, or a UUID and a
    tag -- the transaction numbers it holds, as disjoint closed intervals.

    Every transaction the stream reads goes in, whether or not it touched a
    tracked table, so a resume asks the server for exactly the transactions
    it does not hold, and one whose history spans several server UUIDs
    keeps all of them."""

    def __init__(self) -> None:
        self._intervals: dict[str, list[tuple[int, int]]] = {}

    @classmethod
    def parse(cls, text: str) -> "GtidSet":
        gtid_set = cls()
        for element in "".join(text.split()).split(","):
            if not element:
                continue
            uuid, _, rest = element.partition(":")
            source = uuid.lower()
            for part in rest.split(":"):
                match = _GTID_INTERVAL.match(part)
                if match is None:
                    source = f"{uuid.lower()}:{part}"
                    continue
                first = int(match.group(1))
                last = int(match.group(2) or first)
                gtid_set._add(source, first, last)
        return gtid_set

    @staticmethod
    def split(gtid: str) -> tuple[str, int]:
        """A single GTID as its source and transaction number."""
        source, _, number = gtid.rpartition(":")
        uuid, colon, tag = source.partition(":")
        return f"{uuid.lower()}{colon}{tag}", int(number)

    def holds(self, source: str) -> bool:
        return source in self._intervals

    def add(self, gtid: str) -> None:
        source, number = self.split(gtid)
        self._add(source, number, number)

    def add_up_to(self, gtid: str) -> None:
        """Hold every transaction of the GTID's source up to and including it."""
        source, number = self.split(gtid)
        self._add(source, 1, number)

    def with_gtid(self, gtid: str) -> "GtidSet":
        copy = GtidSet()
        copy._intervals = {k: list(v) for k, v in self._intervals.items()}
        copy.add(gtid)
        return copy

    def _add(self, source: str, first: int, last: int) -> None:
        merged: list[tuple[int, int]] = []
        for start, end in sorted([*self._intervals.get(source, []), (first, last)]):
            if merged and start <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        self._intervals[source] = merged

    def __str__(self) -> str:
        return ",".join(
            source
            + "".join(
                f":{start}" if start == end else f":{start}-{end}"
                for start, end in intervals
            )
            for source, intervals in sorted(self._intervals.items())
        )


class GtidOffset(CdcOffset):
    """MySQL CDC offset = the GTID set the binlog stream has
    consumed so far (see :class:`GtidSet`). Empty string is the
    cold-start marker.

    It also records the stream's source: the server UUID (and tag) its
    transactions come from, once the first one has been read. Transaction
    numbers only order the changes of one source, so a stream reads one
    source only (see :meth:`BinlogIngester._begin_transaction`)."""

    def __init__(self, gtid: str = "", source: str | None = None):
        self.gtid = gtid
        self.source = source

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, GtidOffset):
            raise MysqlErrorCode.MYSQL_17.exception(
                actual=type(other).__name__,
            )
        self.gtid = other.gtid
        if other.source is not None:
            self.source = other.source

    def write(self) -> str:
        return json.dumps({"gtid": self.gtid, "source": self.source})

    @classmethod
    def read(cls, data: str) -> "GtidOffset":
        try:
            payload = json.loads(data)
            gtid, source = payload["gtid"], payload["source"]
            if not isinstance(gtid, str) or not (
                source is None or isinstance(source, str)
            ):
                raise TypeError("gtid or source mistyped")
        except (TypeError, ValueError, KeyError) as error:
            raise MysqlErrorCode.MYSQL_24.exception(cause=error, state=data)
        return cls(gtid=gtid, source=source)

    def copy(self) -> "GtidOffset":
        return GtidOffset(gtid=self.gtid, source=self.source)


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


# What the stream reads from the binlog: row changes (a statement-based binlog
# has no rows), every column of them (MINIMAL and NOBLOB leave out columns an
# update did not change), and a GTID for each transaction, which the offset is.
REQUIRED_BINLOG_SETTINGS = {
    "log_bin": {"ON", "1"},
    "binlog_format": {"ROW"},
    "binlog_row_image": {"FULL"},
    "gtid_mode": {"ON"},
}


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
        poll_max_secs: int = DEFAULT_POLL_MAX_SECS,
    ):
        super().__init__(tables=tables)
        self.offsets = TabsdataKeptOffsets(
            key=CDC_OFFSET_STATE_KEY,
            read=GtidOffset.read,
        )
        self._conn = conn
        self._start_from = start_from
        self._server_id = server_id
        self._poll_max_secs = poll_max_secs
        # ``_mark`` advances as GTID events flow past; ``_base_offset``
        # is the snapshot taken at stream init for the runner.
        self._mark: GtidOffset | None = None
        self._base_offset: GtidOffset | None = None
        self._stream: BinLogStreamReader | None = None
        # A poll that runs out of time on the event opening a transaction, or
        # while one is still open, has already taken that event off the
        # stream, so its GTID is held here for the next poll to adopt. Losing
        # it would tag the whole transaction with an empty GTID, and segment 0
        # sits below every real one, and would leave it out of the offset.
        self._carry_gtid: str | None = None
        self._warned_zone = False
        self._warned_commit_time = False
        # Every transaction read so far, seeded with those before the start.
        self._executed = GtidSet()
        # A start by binlog position or timestamp has no set to seed from, so
        # a source's first transaction stands for all its earlier ones.
        self._seed_on_first_sight = False
        # The server UUID (and tag) every transaction must come from, once
        # known: recorded by the first one, and kept across a reload.
        self._source: str | None = None

    @property
    def ingester_type(self) -> CdcIngesterType:
        return "binlog"

    @property
    def dialect(self) -> MySQLDialect:
        return MySQLDialect()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(self, work_dir: Path) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("MySQL server timezone: %s", self._server_tz)
        self.init_stream()
        return engine

    def open_initial_load(self) -> BinlogInitialLoadSession:
        return BinlogInitialLoadSession(connection_settings=self._connection_settings())

    def close_session(self, session: Any) -> None:
        if self._stream is not None:
            try:
                self._stream.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to close binlog stream: %s", exc)
            finally:
                self._stream = None
                self._carry_gtid = None
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
        url = make_url(normalize_mysql_cdc_uri(self._conn._uri()))
        creds = self._conn.credentials
        return url.set(
            username=_convert(creds.user, str),
            password=_convert(creds.password, str),
        )

    def _build_engine(self) -> Any:
        return sqlalchemy_utils.create_engine(
            self._resolve_url(), connect_args=self.connect_args
        )

    def _connection_settings(self) -> dict[str, Any]:
        """The pymysql keyword arguments of every connection the stream opens:
        the connection's ``conn_cfg`` (TLS, charset, timeouts) under the
        address and credentials of its URI."""
        url = self._resolve_url()
        settings: dict[str, Any] = {
            **self.connect_args,
            "host": url.host,
            "port": url.port or 3306,
            "user": url.username,
            "passwd": url.password,
        }
        if url.database:
            settings["db"] = url.database
        return settings

    def _stream_connection_settings(self) -> dict[str, Any]:
        settings = self._connection_settings()
        settings["read_timeout"] = int(self._poll_max_secs)
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
            "slave_heartbeat": min(self._poll_max_secs, HEARTBEAT_INTERVAL_SECONDS),
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

        self._seed_on_first_sight = False
        self._verify_server_settings()
        if self._mark is not None and self._mark.gtid:
            # A persisted or initial-load offset is already the full set.
            self._executed = GtidSet.parse(self._mark.gtid)
            start_from = None
        else:
            start_from = self._start_from

        match start_from:
            case None:
                pass
            case GtidPosition(gtid=gtid):
                self._executed = GtidSet.parse(self._as_gtid_set(gtid))
            case BinlogPosition(file=file, pos=pos):
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                self._executed = GtidSet()
                self._seed_on_first_sight = True
            case TimestampPosition(ts=ts):
                since = epoch_seconds(ts)
                file, _ = self._first_binlog_file()
                retained_since = self._binlog_begins_at(file)
                if retained_since > since:
                    raise MysqlErrorCode.MYSQL_29.exception(
                        timestamp=ts.isoformat(),
                        retained=datetime.fromtimestamp(
                            retained_since, tz=timezone.utc
                        ).isoformat(),
                    )
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = BINLOG_START_POSITION
                stream_kwargs["skip_to_timestamp"] = since
                self._executed = GtidSet()
                self._seed_on_first_sight = True
            case "head":
                file, purged = self._first_binlog_file()
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = BINLOG_START_POSITION
                self._executed = GtidSet.parse(purged)
            case "tail":
                file, pos, executed = self._last_binlog_position()
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                self._executed = GtidSet.parse(executed)
            case _:
                raise MysqlErrorCode.MYSQL_14.exception(start_from=start_from)

        if "log_file" not in stream_kwargs and "skip_to_timestamp" not in stream_kwargs:
            consumed = str(self._executed)
            self._verify_gtids(consumed)
            stream_kwargs["auto_position"] = consumed
        self._mark = self._offset_of(self._executed)
        self._base_offset = self._mark.copy()
        self._stream = BinLogStreamReader(**stream_kwargs)
        logger.info("BinLogStreamReader initialised")

    @staticmethod
    def _as_gtid_set(gtid: str) -> str:
        # A user's ``start_from`` may name one GTID (``uuid:N``), meaning
        # everything of that source up to it; a set is taken as it is.
        if not gtid or "," in gtid:
            return gtid
        uuid, _, seq = gtid.rpartition(":")
        if not uuid or "-" in seq:
            return gtid
        return f"{uuid}:1-{seq}"

    def seed_offset(self, offset: CdcOffset | None) -> None:
        # A reload seeds the previous stream's offset before the load's own,
        # which has no source yet: the one recorded is kept.
        if isinstance(offset, GtidOffset) and offset.source is not None:
            self._source = offset.source
        super().seed_offset(offset)

    def _offset_of(self, executed: GtidSet) -> GtidOffset:
        return GtidOffset(gtid=str(executed), source=self._source)

    def _begin_transaction(self, gtid: str) -> GtidOffset:
        """The offset of the transaction `gtid` opens: every one read before
        it, and it.

        Fails when it comes from another source than the stream's. The
        stage publisher keeps only rows past the highest transaction number
        it has published, and numbers are per source: a new source's
        restart low, so its changes would be dropped without a word -- a
        failover to another server, or a stream fed by several."""
        source, number = GtidSet.split(gtid)
        if self._source is None:
            self._source = source
        elif source != self._source:
            raise MysqlErrorCode.MYSQL_25.exception(
                recorded=self._source, actual=source, gtid=gtid
            )
        if self._seed_on_first_sight and not self._executed.holds(source):
            if number > 1:
                self._executed.add_up_to(f"{source}:{number - 1}")
        return self._offset_of(self._executed.with_gtid(gtid))

    def _complete_transaction(self, gtid: str) -> None:
        self._executed.add(gtid)
        self._mark = self._offset_of(self._executed)

    def _verify_server_settings(self) -> None:
        """Fail on a server whose binlog the stream cannot rely on: one
        without a row-based binlog carrying full row images and GTIDs; a
        replica that does not log what it replicates, whose binlog then holds
        none of the changes its source makes; or one that applies replicated
        transactions in parallel without keeping their commit order, whose
        binlog may then hold a source's transaction before an earlier-numbered
        one, which the stage publisher would drop as already published."""
        conn = pymysql.connect(**self._connection_settings())
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "show global variables where variable_name in ("
                    "'log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', "
                    "'log_replica_updates', 'log_slave_updates', "
                    "'replica_parallel_workers', 'slave_parallel_workers', "
                    "'replica_preserve_commit_order', "
                    "'slave_preserve_commit_order')"
                )
                values = {name.lower(): value for name, value in cursor.fetchall()}
                replica = self._is_replica(cursor)
        finally:
            conn.close()
        wrong = [
            f"{name} {values.get(name, 'unset')}"
            for name, accepted in REQUIRED_BINLOG_SETTINGS.items()
            if str(values.get(name, "")).upper() not in accepted
        ]
        if wrong:
            raise MysqlErrorCode.MYSQL_27.exception(settings=", ".join(wrong))
        logged = values.get(
            "log_replica_updates", values.get("log_slave_updates", "OFF")
        )
        if replica and str(logged).upper() not in {"ON", "1"}:
            raise MysqlErrorCode.MYSQL_28.exception(logged=logged)
        workers = values.get(
            "replica_parallel_workers", values.get("slave_parallel_workers", "0")
        )
        preserved = values.get(
            "replica_preserve_commit_order",
            values.get("slave_preserve_commit_order", "OFF"),
        )
        if int(workers or 0) > 1 and str(preserved).upper() != "ON":
            raise MysqlErrorCode.MYSQL_26.exception(
                workers=workers, preserved=preserved
            )

    @staticmethod
    def _is_replica(cursor: Any) -> bool:
        """Whether the server has a replication source configured: from
        CHANGE REPLICATION SOURCE TO on, running or not, the status holds a
        row per channel, and none before. MySQL before 8.0.22 knows the
        statement only by its older name."""
        try:
            cursor.execute("show replica status")
        except pymysql.err.ProgrammingError:
            cursor.execute("show slave status")
        return bool(cursor.fetchall())

    @staticmethod
    def _transaction_number(gtid: str | None) -> int:
        if not gtid:
            return 0
        _, _, number = gtid.rpartition(":")
        return int(number)

    def _verify_gtids(self, consumed: str) -> None:
        """Fail unless the server can send every transaction after
        `consumed`: it has purged none that the stream has not read, and it
        has executed every one the stream has."""
        conn = pymysql.connect(**self._connection_settings())
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "select @@global.gtid_purged, @@global.gtid_executed, "
                    "gtid_subset(@@global.gtid_purged, %s), "
                    "gtid_subset(%s, @@global.gtid_executed)",
                    (consumed, consumed),
                )
                purged, executed, purged_read, consumed_executed = cursor.fetchone()
        finally:
            conn.close()
        if not purged_read:
            raise MysqlErrorCode.MYSQL_22.exception(purged=purged, consumed=consumed)
        if not consumed_executed:
            raise MysqlErrorCode.MYSQL_23.exception(
                consumed=consumed, executed=executed
            )

    def _binlog_begins_at(self, file: str) -> int:
        reader = BinLogStreamReader(
            connection_settings=self._stream_connection_settings(),
            server_id=self._server_id,
            log_file=file,
            log_pos=BINLOG_START_POSITION,
            blocking=False,
            resume_stream=False,
            only_events=[FormatDescriptionEvent],
        )
        try:
            event = reader.fetchone()
            return event.timestamp if event is not None else 0
        finally:
            reader.close()

    def _first_binlog_file(self) -> tuple[str, str]:
        conn = pymysql.connect(**self._connection_settings())
        try:
            with conn.cursor() as cursor:
                cursor.execute("show binary logs")
                row = cursor.fetchone()
                if not row:
                    raise MysqlErrorCode.MYSQL_15.exception(position="head")
                # Transactions before the first binlog still held are exactly
                # the purged ones.
                cursor.execute("select @@global.gtid_purged")
                return row[0], cursor.fetchone()[0] or ""
        finally:
            conn.close()

    def _last_binlog_position(self) -> tuple[str, int, str]:
        conn = pymysql.connect(**self._connection_settings())
        try:
            with conn.cursor() as cursor:
                row = self._query_binlog_status(cursor)
                if not row:
                    raise MysqlErrorCode.MYSQL_15.exception(position="tail")
                # Executed_Gtid_Set, read with the position it belongs to.
                return row[0], row[1], row[4] or ""
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

    # ------------------------------------------------------------------
    # Schema extraction
    # ------------------------------------------------------------------

    def _truncate_change(
        self, event: Any, gtid: str, offset: GtidOffset
    ) -> CdcDataChange | None:
        match = _TRUNCATE_STATEMENT.match((event.query or "").strip())
        if match is None:
            return None
        schema = match.group("schema") or self._event_schema(event)
        table = match.group("table")
        qualified = self._as_qualified_table(
            _unquote_identifier(schema), _unquote_identifier(table)
        )
        if qualified is None or self._schema_registry is None:
            return None
        schema_version = (
            self._schema_registry.latest(qualified) or self._schema_registry.unknown()
        )
        null_row = {column.name: None for column in schema_version.columns}
        return CdcDataChange(
            operation=CdcOperation.TRUNCATE,
            transaction=gtid,
            sequence=0,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
            table=qualified,
            new_values=dict(null_row),
            old_values=dict(null_row),
            offset=offset,
            segment=self._transaction_number(gtid),
            ordinal=0,
            schema=schema_version,
        )

    @staticmethod
    def _event_schema(event: Any) -> str:
        schema = getattr(event, "schema", "") or ""
        return schema.decode() if isinstance(schema, bytes) else schema

    def _timestamp_columns(self, event: Any) -> frozenset[str]:
        return frozenset(
            column.name for column in event.columns if column.type in _TIMESTAMP_CODES
        )

    def _to_server_clock(self, values: dict | None, stamps: frozenset[str]) -> None:
        """A TIMESTAMP column reaches the binlog as an instant and its reader
        hands it over as a naive UTC reading; a batch read of the same row
        gives the server zone's wall clock, so the stream does the same. Without
        a known server zone the UTC reading stays, said once."""
        if not values or not stamps:
            return
        if self._server_tz is None:
            if not self._warned_zone:
                self._warned_zone = True
                logger.warning(
                    "The MySQL server zone is unknown; TIMESTAMP values are "
                    "published as UTC wall clock"
                )
            return
        for name in stamps:
            value = values.get(name)
            if isinstance(value, datetime) and value.tzinfo is None:
                values[name] = (
                    value
                    .replace(tzinfo=timezone.utc)
                    .astimezone(self._server_tz)
                    .replace(tzinfo=None)
                )

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
            if getattr(column, "length_size", None) is not None:
                parts.append(f"lensize={column.length_size}")
            if getattr(column, "character_set_name", None) is not None:
                parts.append(f"charset={column.character_set_name}")
            schema.append((name, ":".join(parts), column))
        return tuple(schema)

    # ------------------------------------------------------------------
    # Poll loop body
    # ------------------------------------------------------------------

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        # ``deadline`` caps the time we'll spend in a single poll
        # before returning control to the runner. Without it,
        # blocking=True + a stream that's idle would hang the
        # runner forever.
        deadline = time.monotonic() + self._poll_max_secs
        gtid: str | None = self._carry_gtid
        self._carry_gtid = None
        offset = self._begin_transaction(gtid) if gtid is not None else None
        committed_at: datetime | None = None
        seq: int = 0
        if self._stream is None:
            return
        for event in self._stream:
            if isinstance(event, GtidEvent):
                if gtid is not None and seq == 0:
                    # The transaction before ended with no commit event of its
                    # own, which a statement the stream does not see does.
                    self._complete_transaction(gtid)
                    gtid = offset = None
                if seq == 0 and time.monotonic() > deadline:
                    self._carry_gtid = event.gtid
                    return
                gtid = event.gtid
                offset = self._begin_transaction(gtid)
                committed_at = self._committed_at(event)
                seq = 0
            elif isinstance(event, (XidEvent, QueryEvent)):
                if isinstance(event, QueryEvent):
                    truncated = self._truncate_change(
                        event,
                        gtid or "",
                        offset or self._offset_of(self._executed),
                    )
                    if truncated is not None:
                        seq += 1
                        truncated.sequence = truncated.ordinal = seq
                        yield truncated
                        if gtid is not None:
                            self._complete_transaction(gtid)
                        gtid = offset = None
                        seq = 0
                        continue
                if _opens_transaction(event):
                    continue
                if not _commits_transaction(event):
                    # A statement that commits by itself (DDL): its transaction
                    # holds no rows, so it is complete.
                    if gtid is not None and seq == 0:
                        self._complete_transaction(gtid)
                        gtid = offset = None
                        seq = 0
                    continue
                if gtid is not None:
                    self._complete_transaction(gtid)
                gtid = offset = None
                seq = 0
            elif isinstance(event, HeartbeatLogEvent):
                if seq == 0 and time.monotonic() > deadline:
                    self._carry_gtid = gtid
                    return
            elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                qualified = self._as_qualified_table(event.schema, event.table)
                if qualified is None:
                    continue
                op = _OPERATION_MAP[type(event)]
                timestamp = committed_at or datetime.fromtimestamp(
                    event.timestamp, tz=timezone.utc
                )
                raw_schema = self._extract_raw_schema(event)
                if self._schema_registry is None:  # pragma: no cover
                    return
                schema_version = self._schema_registry.register_from_log(
                    qualified,
                    raw_schema,
                )
                stamps = self._timestamp_columns(event)
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
                    self._to_server_clock(new_values, stamps)
                    self._to_server_clock(old_values, stamps)
                    yield CdcDataChange(
                        operation=op,
                        transaction=gtid or "",
                        sequence=seq,
                        timestamp=timestamp,
                        table=qualified,
                        new_values=new_values,
                        old_values=old_values,
                        offset=offset or self._offset_of(self._executed),
                        segment=self._transaction_number(gtid),
                        ordinal=seq,
                        schema=schema_version,
                    )

    def _committed_at(self, event: Any) -> datetime | None:
        """The commit instant every row of the transaction ``event`` opens is
        stamped with. A server too old to write it leaves each row its own
        statement time, which can precede the commit by as long as the
        transaction ran, said once."""
        committed_at = _commit_timestamp(event)
        if committed_at is None and not self._warned_commit_time:
            self._warned_commit_time = True
            logger.warning(
                "The MySQL binlog carries no commit instant (server older than "
                "8.0.1); every row is published with its statement time"
            )
        return committed_at
