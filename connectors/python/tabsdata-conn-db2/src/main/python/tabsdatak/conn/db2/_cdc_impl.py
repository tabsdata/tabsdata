#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 CDC implementation -- dialect + SQL Replication ingester.

Mirrors the layering of the batch side (``_impl.py``): the
connector front (``Db2CdcStreamSrcConn`` / ``Db2CdcStreamSrc`` in
``__init__.py``) and the plugin glue (``_cdc_plugin.py``) sit above
this module; this is where the sqlalchemy + IBM SQL Replication
machinery lives.

Three pieces:

* :class:`Db2Dialect` -- implements the dialect-agnostic
  ``Dialect`` abstract for catalog lookups, identifier quoting
  (double quotes, ``""`` escape), upper-case folding,
  large-column classification (BLOB/CLOB/XML), and a passthrough
  ``cdc_to_sa`` for the ``sqlreplica`` ingester (the schema
  introspection already returns SQLAlchemy types).
* :class:`SqlReplicaIngester` (+ :class:`CommitSeqOffset`) -- the
  ``CdcIngester`` instance that polls the IBM SQL Replication
  ``CD`` (change-data) tables and emits ``CdcDataChange`` records
  for the runner. Per-table offsets advance on each poll.
* :func:`match_cd_columns` -- pairs the CD table's after-image
  columns with their before-image siblings (named with the
  ``IBMSNAP_REGISTER.before_img_prefix``) so UPDATEs carry both
  pre- and post-values.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
from typing import Any, Literal, TypeAlias, Union

import sqlalchemy as sa
import sqlalchemy.types as sa_types
from sqlalchemy import text
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import CLOB, LargeBinary, TypeEngine

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._ingester import CdcIngester
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    InitialLoadSession,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_FETCH_SIZE,
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
    TimestampPosition as _CommonTimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    TableCommitSeqPosition,
)
from tabsdatak.conn.db2.error import Db2ErrorCode

logger = logging.getLogger(__name__)


# --- Db2-specific runtime constants ---------------------------------

# Schema that hosts the IBM SQL Replication capture tables
# (IBMSNAP_REGISTER, IBMSNAP_UOW, ...).
ASN_SCHEMA = "ASN"

POLL_INTERVAL_SECONDS = 1.0

# 16 zero bytes -- the "before any commit" baseline IBMSNAP uses.
COMMITSEQ_ORIGIN = b"\x00" * 16

# The seg and ord columns are decimal128, which holds 38 decimal digits. A full
# 16-byte IBMSNAP_COMMITSEQ or IBMSNAP_INTENTSEQ can in theory reach 39 digits;
# guard so an out-of-range value fails loudly rather than silently overflowing.
POSITION_MAX_DIGITS = 38
POSITION_LIMIT = 10**POSITION_MAX_DIGITS


# Internal start-position shape consumed by the ingester after the
# plugin has converted the user-facing ``Db2CdcStartFromSpec``.
# ``TimestampPosition`` here is the common runner type;
# ``CommitSeqPosition`` / ``TableCommitSeqPosition`` are db2-local.
_InternalStartFromSpec: TypeAlias = Union[
    Literal["head", "tail"],
    CommitSeqPosition,
    TableCommitSeqPosition,
    _CommonTimestampPosition,
]


# IBMSNAP_OPERATION single-letter codes -> CdcOperation discriminator.
_OPERATION_MAP = {
    "I": CdcOperation.INSERT,
    "U": CdcOperation.UPDATE,
    "D": CdcOperation.DELETE,
}


# CD tables prepend these system columns to every row event.
@dataclass(frozen=True)
class _SystemColumn:
    db_name: str
    sa_name: str | None = None

    @property
    def quoted(self) -> str:
        return f'"{self.db_name}"'


IBMSNAP_OPERATION = _SystemColumn("IBMSNAP_OPERATION")
IBMSNAP_COMMITSEQ = _SystemColumn("IBMSNAP_COMMITSEQ")
IBMSNAP_INTENTSEQ = _SystemColumn("IBMSNAP_INTENTSEQ")
IBMSNAP_LOGMARKER = _SystemColumn("IBMSNAP_LOGMARKER")

_SYSTEM_COLUMNS = (
    IBMSNAP_OPERATION,
    IBMSNAP_COMMITSEQ,
    IBMSNAP_INTENTSEQ,
    IBMSNAP_LOGMARKER,
)

_CD_SYSTEM_COLUMN_NAMES = frozenset(c.db_name for c in _SYSTEM_COLUMNS)


# ---------------------------------------------------------------------
# Dialect
# ---------------------------------------------------------------------

# SQL Replication does not copy LOB or XML values into the CD tables, so
# every Db2 large type is of the "object" kind. LONG VARCHAR reflects as
# a VARCHAR subclass and is a regular column.
try:
    from ibm_db_sa import DBCLOB, XML

    _LARGE_OBJECT_TYPES = (LargeBinary, CLOB, DBCLOB, XML)
except ImportError:
    _LARGE_OBJECT_TYPES = (LargeBinary, CLOB)


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


class Db2Dialect(Dialect):
    """``Dialect`` for the Db2 SQL Replication CDC ingester."""

    @property
    def open_quote(self) -> str:
        return '"'

    @property
    def close_quote(self) -> str:
        return '"'

    def default_case_fold(self, name: str) -> str:
        # Db2 folds unquoted identifiers to upper case.
        return name.upper()

    def default_schema(self, inspector: sa.engine.reflection.Inspector) -> str:
        # Db2's effective schema for unqualified names is CURRENT SCHEMA, which
        # honours the connection's `currentSchema=` attribute. SQLAlchemy's
        # reported default_schema_name is the authorization-id schema instead,
        # so query CURRENT SCHEMA directly.
        with inspector.bind.connect() as connection:
            schema = connection.execute(
                text("select current schema from sysibm.sysdummy1")
            ).scalar()
        return schema.strip() if isinstance(schema, str) else schema

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

        # Unqualified: the schema is filled from the connection's CURRENT SCHEMA
        # by the caller (see default_schema).
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
            row = connection.execute(
                text("select current timezone from sysibm.sysdummy1")
            ).fetchone()
        if row and row[0] is not None:
            return _parse_offset_string(str(row[0]))
        return None

    def large_column_kind(self, satype: Any) -> LargeColumnKind | None:
        if isinstance(satype, _LARGE_OBJECT_TYPES):
            return "object"
        return None

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select colname "
                "from syscat.columns "
                "where tabschema = :schema "
                "and tabname = :table "
                "order by colno"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
        return [row[0].strip() for row in rows]

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        if ingester_type == "sqlreplica":
            # SqlReplicaIngester already returns SA types from the
            # CD-table introspection -- no mapping needed.
            return cdc_dtype
        raise NotImplementedError(
            f"{type(self).__name__}.cdc_to_sa not implemented for "
            f"ingester_type={ingester_type!r}"
        )


# ---------------------------------------------------------------------
# Matching CD columns to their before-image siblings
# ---------------------------------------------------------------------


@dataclass
class MatchedColumns:
    after_columns_list: list[tuple[str, TypeEngine]]
    before_columns_map: dict[str, str]


def match_cd_columns(
    cd_columns: list[tuple[str, TypeEngine]],
    before_column_prefix: str | None,
    *,
    cd_schema: str | None = None,
    cd_table: str | None = None,
) -> MatchedColumns:
    """Pair after-image columns with their before-image siblings.

    The CD table's columns come in two flavours after dropping the
    IBMSNAP_* system columns: the "after image" (user-visible
    column names) and the "before image" (the same names prefixed
    by ``IBMSNAP_REGISTER.before_img_prefix``). A registration
    without a configured prefix means UPDATE pre-images cannot be
    matched and we surface that loudly.

    Raises:
        DB2_18 if ``before_column_prefix`` is empty.
        DB2_19 / DB2_20 if there are orphan before- or after-image
        columns (a prefix mismatch the matcher cannot reconcile).
    """
    user_columns = [
        (name, sa_type)
        for name, sa_type in cd_columns
        if name.upper() not in _CD_SYSTEM_COLUMN_NAMES
    ]

    if not before_column_prefix:
        raise Db2ErrorCode.DB2_18.exception(
            cd_owner=cd_schema,
            cd_table=cd_table,
        )

    prefix = before_column_prefix
    prefix_len = len(prefix)

    user_columns_map: dict[str, TypeEngine] = {
        name: sa_type for name, sa_type in user_columns
    }
    user_columns_set = set(user_columns_map.keys())

    # Sort by length so prefix candidates are considered after
    # their potential after-image targets.
    sorted_user_columns = sorted(user_columns_set, key=lambda n: (len(n), n))

    matched_before: set[str] = set()
    matched_after: set[str] = set()
    before_columns_map: dict[str, str] = {}

    for name in sorted_user_columns:
        if name in matched_before or name in matched_after:
            continue
        if name.startswith(prefix) and len(name) > prefix_len:
            candidate_after = name[prefix_len:]
            if (
                candidate_after in user_columns_set
                and candidate_after not in matched_after
                and candidate_after not in matched_before
            ):
                matched_before.add(name)
                matched_after.add(candidate_after)
                before_columns_map[candidate_after] = name

    unmatched_before: list[str] = []
    unmatched_after: list[str] = []

    for name in sorted_user_columns:
        if name in matched_before or name in matched_after:
            continue
        if name.startswith(prefix) and len(name) > prefix_len:
            unmatched_before.append(name)
        else:
            unmatched_after.append(name)

    if unmatched_before:
        raise Db2ErrorCode.DB2_19.exception(
            unmatched_before=unmatched_before,
            unmatched_after=unmatched_after,
            prefix=prefix,
        )
    if unmatched_after:
        raise Db2ErrorCode.DB2_20.exception(
            unmatched_after=unmatched_after,
            prefix=prefix,
        )

    after_names = matched_after
    new_columns = [
        (name, sa_type) for name, sa_type in user_columns if name in after_names
    ]
    return MatchedColumns(
        after_columns_list=new_columns,
        before_columns_map=before_columns_map,
    )


# ---------------------------------------------------------------------
# Offset
# ---------------------------------------------------------------------


class CommitSeqOffset(CdcOffset):
    """Db2 CDC offset = the per-CD-table IBMSNAP_COMMITSEQ value the
    ingester has emitted up to. Each tracked CD table has its own
    advancing watermark; the offset state is therefore a dict
    keyed by qualified CD-table name."""

    def __init__(self, commits: dict[str, bytes] | None = None):
        self.commits: dict[str, bytes] = dict(commits) if commits else {}

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, CommitSeqOffset):
            raise Db2ErrorCode.DB2_21.exception(
                actual=type(other).__name__,
            )
        self.commits.update(other.commits)

    def write(self) -> str:
        # Serialise the per-table map with binary commitseqs as hex strings.
        return json.dumps({
            table: commitseq.hex() for table, commitseq in self.commits.items()
        })

    @classmethod
    def read(cls, data: str) -> "CommitSeqOffset":
        payload = json.loads(data) if data else {}
        return cls(
            commits={table: bytes.fromhex(seq) for table, seq in payload.items()}
        )

    def copy(self) -> "CommitSeqOffset":
        return CommitSeqOffset(commits=self.commits)


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class SqlReplicaInitialLoadSession(InitialLoadSession):
    """Snapshot pinned to a capture position by locking the source tables
    in share mode, reading the highest commit sequence the capture program
    has recorded, then releasing the locks and reading the rows under
    repeatable read.

    The share locks let readers through and block only writers, and only
    while the position is read. The position is the global
    ``max(IBMSNAP_COMMITSEQ)`` across units of work, so a change committed
    after it is captured above the mark rather than lost; a change
    committed just before it may be replayed, which is the safe direction.
    """

    def __init__(self, *, engine: Any, dialect: Db2Dialect):
        self._engine = engine
        self._dialect = dialect
        self._offset: CommitSeqOffset | None = None
        self._tracked_tables: dict[str, TrackedTable] = {}

    @property
    def engine(self) -> Any:
        return self._engine

    @property
    def offset(self) -> CdcOffset:
        if self._offset is None:
            raise Db2ErrorCode.DB2_25.exception()
        return self._offset

    def tables(
        self, tracked_tables: dict[str, TrackedTable]
    ) -> Iterator[tuple[TrackedTable, Any]]:
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            self._pin(connection, tracked_tables)
            for tracked_table in tracked_tables.values():
                yield tracked_table, connection

    def _pin(self, connection: Any, tracked_tables: dict[str, TrackedTable]) -> None:
        for tracked_table in tracked_tables.values():
            data_table = tracked_table.data_table
            qualified = (
                f"{self._dialect.quote_identifier(data_table.schema)}"
                f".{self._dialect.quote_identifier(data_table.table)}"
            )
            connection.execute(text(f"lock table {qualified} in share mode"))
        commitseq = connection.execute(
            text(
                f"select max({IBMSNAP_COMMITSEQ.quoted}) from {ASN_SCHEMA}.IBMSNAP_UOW"
            )
        ).scalar()
        commitseq = commitseq or COMMITSEQ_ORIGIN
        self._offset = CommitSeqOffset(
            commits={
                tracked_table.cdc_table.qualified: commitseq
                for tracked_table in tracked_tables.values()
            }
        )
        connection.commit()
        logger.info(
            "Initial load snapshot is consistent at commit sequence %s",
            commitseq.hex(),
        )

    def close(self) -> None:
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose initial-load engine: %s", exc)
            self._engine = None


class SqlReplicaIngester(CdcIngester):
    """``CdcIngester`` for Db2 via IBM SQL Replication.

    SQL Replication captures source-table changes into shadow CD
    tables; the IBMSNAP capture program populates them in the
    background. This ingester polls the CD tables for rows whose
    ``IBMSNAP_COMMITSEQ`` is past the per-table mark, ordered by
    ``(IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ)`` -- this is the
    same global order the apply program sees.
    """

    def __init__(
        self,
        # Db2CdcStreamSrcConn duck-typed (the ingester only reads
        # ``uri`` / ``credentials`` / ``conn_cfg``); avoids the
        # __init__.py -> _cdc_impl.py cycle.
        conn: Any,
        tables: QualifiedTablesSpec,
        start_from: _InternalStartFromSpec,
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        fetch_size: int = DEFAULT_FETCH_SIZE,
    ):
        super().__init__(tables=tables)
        self._conn = conn
        self._start_from = start_from
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._fetch_size = fetch_size
        self._mark: CommitSeqOffset | None = None
        self._base_offset: CommitSeqOffset | None = None
        self._before_img_prefixes: dict[str, str | None] = {}

    @property
    def ingester_type(self) -> CdcIngesterType:
        return "sqlreplica"

    @property
    def dialect(self) -> Db2Dialect:
        return Db2Dialect()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(self) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("Db2 server timezone: %s", self._server_tz)
        return engine

    def open_initial_load(self) -> SqlReplicaInitialLoadSession:
        return SqlReplicaInitialLoadSession(
            engine=self._build_engine(),
            dialect=self.dialect,
        )

    def close_session(self, session: Any) -> None:
        if session is not None:
            try:
                session.dispose()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to dispose SA engine: %s", exc)

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _resolve_url(self):
        from sqlalchemy import make_url

        url = make_url(self._conn._uri())
        creds = self._conn.credentials
        return url.set(
            username=_convert(creds.user, str),
            password=_convert(creds.password, str),
        )

    def _build_engine(self) -> Any:
        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import sqlalchemy_utils

        return sqlalchemy_utils.create_engine(self._resolve_url())

    # ------------------------------------------------------------------
    # Source-table resolution + structure (override of base helpers)
    # ------------------------------------------------------------------

    def gather_table_identity(
        self,
        tablespec: str,
        inspector: sa.engine.reflection.Inspector,
    ) -> TrackedTable:
        """``tablespec`` here is a CD-table FQN. Look up the
        registered source via ASN.IBMSNAP_REGISTER and seed a
        ``TrackedTable`` whose ``cdc_table`` points at the CD
        table and ``data_table`` points at the source."""
        from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode

        sa_dialect = inspector.bind.dialect
        try:
            cd_schema, cd_table = self.dialect.parse_qualified_table(tablespec)
        except ValueError as error:
            raise SqlCdcErrorCode.CDC_1.exception(
                cause=error,
                spec=tablespec,
            )

        # An unqualified CD table resolves against the connection's CURRENT
        # SCHEMA, just as batch SQL resolves an unqualified name.
        if cd_schema is None:
            default = self.dialect.default_schema(inspector)
            if not default:
                raise SqlCdcErrorCode.CDC_1.exception(
                    cause=ValueError(
                        "unqualified table and the connection has no default "
                        "schema to qualify it with"
                    ),
                    spec=tablespec,
                )
            real_cd_schema = default
        else:
            real_cd_schema = self.resolve_identifier(cd_schema)
        real_cd_table = self.resolve_identifier(cd_table)

        with inspector.bind.connect() as connection:
            registration = connection.execute(
                text(
                    "select "
                    "source_owner, "
                    "source_table, "
                    "before_img_prefix "
                    f"from {ASN_SCHEMA}.ibmsnap_register "
                    "where cd_owner = :cd_owner "
                    "and cd_table = :cd_table"
                ),
                {"cd_owner": real_cd_schema, "cd_table": real_cd_table},
            ).fetchone()

        if registration is None:
            raise Db2ErrorCode.DB2_16.exception(
                cd_owner=real_cd_schema,
                cd_table=real_cd_table,
            )

        source_owner = registration[0].strip()
        source_table = registration[1].strip()
        before_img_prefix = registration[2].strip() if registration[2] else None

        normalized_source_schema = sa_dialect.normalize_name(
            quoted_name(source_owner, quote=False)
        )
        normalized_source_table = sa_dialect.normalize_name(
            quoted_name(source_table, quote=False)
        )

        if not inspector.has_table(
            table_name=normalized_source_table,
            schema=normalized_source_schema,
        ):
            raise Db2ErrorCode.DB2_17.exception(
                schema=source_owner,
                table=source_table,
            )

        data_table = TableObject(schema=source_owner, table=source_table)
        cdc_table = TableObject(schema=real_cd_schema, table=real_cd_table)
        tracked = TrackedTable(data_table=data_table, cdc_table=cdc_table)
        self._before_img_prefixes[data_table.qualified] = before_img_prefix
        return tracked

    def gather_table_structure(
        self,
        tracked_table: TrackedTable,
        inspector: sa.engine.reflection.Inspector,
        before_img_prefix: str | None = None,
    ) -> None:
        prefix = (
            before_img_prefix
            or self._before_img_prefixes[tracked_table.data_table.qualified]
        )
        super().gather_table_structure(tracked_table, inspector, prefix)

    def prepare_inspector(
        self,
        inspector: sa.engine.reflection.Inspector,
    ) -> None:
        """Resolve the SQLAlchemy-normalised names of the IBMSNAP
        system columns so ``poll_changes`` can index the row
        result by the same name SA gave each column."""
        sa_dialect = inspector.bind.dialect
        # Replace the system-column dataclasses with versions that
        # carry the resolved ``sa_name`` for this dialect.
        global IBMSNAP_OPERATION, IBMSNAP_COMMITSEQ
        global IBMSNAP_INTENTSEQ, IBMSNAP_LOGMARKER
        IBMSNAP_OPERATION = _SystemColumn(
            db_name=IBMSNAP_OPERATION.db_name,
            sa_name=sa_dialect.normalize_name(IBMSNAP_OPERATION.db_name),
        )
        IBMSNAP_COMMITSEQ = _SystemColumn(
            db_name=IBMSNAP_COMMITSEQ.db_name,
            sa_name=sa_dialect.normalize_name(IBMSNAP_COMMITSEQ.db_name),
        )
        IBMSNAP_INTENTSEQ = _SystemColumn(
            db_name=IBMSNAP_INTENTSEQ.db_name,
            sa_name=sa_dialect.normalize_name(IBMSNAP_INTENTSEQ.db_name),
        )
        IBMSNAP_LOGMARKER = _SystemColumn(
            db_name=IBMSNAP_LOGMARKER.db_name,
            sa_name=sa_dialect.normalize_name(IBMSNAP_LOGMARKER.db_name),
        )

    def _extract_raw_schema(
        self,
        session: Any,
        tracked: TrackedTable,
        before_img_prefix: str | None,
    ) -> tuple[tuple[str, str, Any], ...]:
        """Reflect the CD table's columns, match before/after pairs,
        and populate ``tracked.columns`` + the SA-name lookup the
        ingester uses to index rows by either image."""
        inspector = sa.inspect(session)
        sa_dialect = inspector.bind.dialect
        normalized_schema = sa_dialect.normalize_name(
            quoted_name(tracked.cdc_table.schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(
            quoted_name(tracked.cdc_table.table, quote=False)
        )
        sa_columns = inspector.get_columns(
            normalized_table,
            schema=normalized_schema,
        )

        with inspector.bind.connect() as connection:
            db_columns = self.dialect.get_catalog_columns(
                connection,
                tracked.cdc_table.schema,
                tracked.cdc_table.table,
            )

        cd_columns = [
            (db_column.strip(), sa_column["type"])
            for db_column, sa_column in zip(db_columns, sa_columns)
        ]
        matched = match_cd_columns(
            cd_columns,
            before_img_prefix,
            cd_schema=tracked.cdc_table.schema,
            cd_table=tracked.cdc_table.table,
        )

        columns = []
        for name, sa_type in matched.after_columns_list:
            oname = matched.before_columns_map.get(name)
            columns.append(
                TrackedColumn(
                    name=name,
                    satype=sa_type,
                    oname=oname,
                    sa_name=sa_dialect.normalize_name(name),
                    sa_oname=(sa_dialect.normalize_name(oname) if oname else None),
                )
            )
        tracked.columns = columns
        tracked.build_pa_dtypes()
        tracked.pk = self.gather_table_pk(
            inspector,
            normalized_table,
            normalized_schema,
            sa_columns,
            db_columns,
        )
        return tuple(
            (name, repr(sa_type), sa_type)
            for name, sa_type in matched.after_columns_list
        )

    # ------------------------------------------------------------------
    # Offset management
    # ------------------------------------------------------------------

    def base_offset(self) -> CdcOffset:
        return (self._base_offset or CommitSeqOffset()).copy()

    def commit_offset(self, offset: CdcOffset) -> None:
        if not isinstance(offset, CommitSeqOffset):
            raise Db2ErrorCode.DB2_21.exception(
                actual=type(offset).__name__,
            )
        # SQL Replication has no acknowledgement protocol; the
        # capture program advances independently. The mark stays
        # local to the runner -- no-op here, just type-check.

    def offset_from_state(self, offset_state: str) -> CdcOffset:
        # SQL Replication keeps no server-side consumer cursor: Tabsdata
        # owns the offset, so resume from the persisted commit-seq map.
        return CommitSeqOffset.read(offset_state)

    # ------------------------------------------------------------------
    # init_reader -- translate start_from to the per-table commit map
    # ------------------------------------------------------------------

    def _init_reader(self, session: Any) -> None:
        if self._mark is not None:
            self._base_offset = self._mark.copy()
            return
        commits: dict[str, bytes] = {}
        match self._start_from:
            case CommitSeqPosition(seq=seq):
                for tracked in self._tracked_tables.values():
                    commits[tracked.cdc_table.qualified] = bytes.fromhex(seq)
            case TableCommitSeqPosition(seqs=seqs):
                for table, seq in seqs.items():
                    commits[table] = bytes.fromhex(seq)
            case _CommonTimestampPosition(ts=ts):
                with session.connect() as connection:
                    row = connection.execute(
                        text(
                            f"select max({IBMSNAP_COMMITSEQ.quoted}) "
                            f"from {ASN_SCHEMA}.IBMSNAP_UOW "
                            f"where {IBMSNAP_LOGMARKER.quoted} <= :ts"
                        ),
                        {"ts": ts},
                    ).fetchone()
                seq = row[0] if row and row[0] else COMMITSEQ_ORIGIN
                for tracked in self._tracked_tables.values():
                    commits[tracked.cdc_table.qualified] = seq
            case "head":
                for tracked in self._tracked_tables.values():
                    commits[tracked.cdc_table.qualified] = COMMITSEQ_ORIGIN
            case "tail":
                with session.connect() as connection:
                    watermarks = self._query_table_watermarks(connection)
                    row = connection.execute(
                        text(
                            f"select max({IBMSNAP_COMMITSEQ.quoted}) "
                            f"from {ASN_SCHEMA}.IBMSNAP_UOW"
                        )
                    ).fetchone()
                globalseq = row[0] if row and row[0] else COMMITSEQ_ORIGIN
                for tracked in self._tracked_tables.values():
                    qualified = tracked.cdc_table.qualified
                    commits[qualified] = watermarks.get(qualified, globalseq)
            case _:
                raise Db2ErrorCode.DB2_15.exception(
                    start_from=self._start_from,
                )
        self._mark = CommitSeqOffset(commits=commits)
        self._base_offset = self._mark.copy()

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _query_table_watermarks(
        self,
        connection: Any,
    ) -> dict[str, bytes]:
        """One UNION ALL query for the per-CD-table
        ``max(IBMSNAP_COMMITSEQ)`` -- the upper bound for the next
        poll iteration. Tables with no rows yield ``COMMITSEQ_ORIGIN``
        and are skipped by the caller."""
        watermarks: dict[str, bytes] = {}
        if not self._tracked_tables:
            return watermarks
        blocks = []
        index_to_qualified: dict[int, str] = {}
        for i, tracked in enumerate(self._tracked_tables.values()):
            cd_schema = tracked.cdc_table.schema
            cd_table = tracked.cdc_table.table
            blocks.append(
                f"select {i} as idx, "
                f"max({IBMSNAP_COMMITSEQ.quoted}) as mcs "
                f'from "{cd_schema}"."{cd_table}"'
            )
            index_to_qualified[i] = tracked.cdc_table.qualified
        query = text(" union all ".join(blocks))
        rows = connection.execute(query).fetchall()
        for row in rows:
            idx, commitseq = row[0], row[1]
            if commitseq and commitseq > COMMITSEQ_ORIGIN:
                watermarks[index_to_qualified[idx]] = commitseq
        return watermarks

    def _query_table_changes(
        self,
        connection: Any,
        tracked: TrackedTable,
        commitseq: bytes,
        watermark: bytes,
        schema_version: Any,
    ) -> Generator[CdcDataChange, None, None]:
        column_names = [f'"{column.name}"' for column in tracked.columns]
        column_inames = [
            f'"{column.oname}"' for column in tracked.columns if column.oname
        ]
        select_columns = (
            [
                IBMSNAP_OPERATION.quoted,
                IBMSNAP_COMMITSEQ.quoted,
                IBMSNAP_INTENTSEQ.quoted,
                IBMSNAP_LOGMARKER.quoted,
            ]
            + column_names
            + column_inames
        )
        select_clause = ", ".join(select_columns)
        cd_qualified = f"{tracked.cdc_table.schema}{DOT}{tracked.cdc_table.table}"
        query = text(
            f"select {select_clause} "
            f"from {cd_qualified} "
            f"where {IBMSNAP_COMMITSEQ.quoted} > :commitseq "
            f"and {IBMSNAP_COMMITSEQ.quoted} <= :watermark "
            f"order by {IBMSNAP_COMMITSEQ.quoted}, "
            f"{IBMSNAP_INTENTSEQ.quoted}"
        )
        result = connection.execute(
            query,
            {"commitseq": commitseq, "watermark": watermark},
        )
        result = result.yield_per(self._fetch_size)
        for row in result:
            row_map = row._mapping
            db_op = str(row_map[IBMSNAP_OPERATION.sa_name]).strip()
            op = _OPERATION_MAP.get(db_op)
            if op is None:
                raise Db2ErrorCode.DB2_22.exception(
                    op=db_op,
                    table=tracked.data_table.qualified,
                )
            commit_seq = row_map[IBMSNAP_COMMITSEQ.sa_name]
            intent_seq = row_map[IBMSNAP_INTENTSEQ.sa_name]
            logmarker: datetime = row_map[IBMSNAP_LOGMARKER.sa_name]
            if logmarker.tzinfo is None:
                logmarker = logmarker.replace(tzinfo=self._server_tz)
            commit_ts = logmarker.astimezone(timezone.utc)
            new_values = {col.name: row_map.get(col.sa_name) for col in tracked.columns}
            self.localize_values(new_values)
            if op == CdcOperation.UPDATE:
                old_values = {}
                for column in tracked.columns:
                    if column.sa_oname and column.sa_oname in row_map:
                        old_values[column.name] = row_map[column.sa_oname]
                    else:
                        old_values[column.name] = None
                self.localize_values(old_values)
            else:
                old_values = {col.name: None for col in tracked.columns}
            segment = int.from_bytes(commit_seq, byteorder="big")
            ordinal = int.from_bytes(intent_seq, byteorder="big")
            if segment >= POSITION_LIMIT:
                raise Db2ErrorCode.DB2_23.exception(
                    commitseq=commit_seq.hex(),
                    digits=POSITION_MAX_DIGITS,
                    table=tracked.data_table.qualified,
                )
            if ordinal >= POSITION_LIMIT:
                raise Db2ErrorCode.DB2_23.exception(
                    commitseq=intent_seq.hex(),
                    digits=POSITION_MAX_DIGITS,
                    table=tracked.data_table.qualified,
                )
            yield CdcDataChange(
                operation=op,
                transaction=commit_seq.hex(),
                sequence=ordinal,
                timestamp=commit_ts,
                table=tracked.data_table.qualified,
                new_values=new_values,
                old_values=old_values,
                offset=CommitSeqOffset(
                    commits={tracked.cdc_table.qualified: commit_seq},
                ),
                segment=segment,
                ordinal=ordinal,
                schema=schema_version,
            )

    def poll_changes(self, session: Any) -> Iterable[CdcChange]:
        # Lazy init_reader on the first poll -- collect_tables
        # populates ``_tracked_tables`` first.
        if self._base_offset is None:
            self._init_reader(session)
        qualified: str | None = None
        deadline = time.monotonic() + self._blocking_timeout_seconds
        with session.connect() as connection:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                watermarks = self._query_table_watermarks(connection)
                for tracked in self._tracked_tables.values():
                    qualified = tracked.cdc_table.qualified
                    commit = self._mark.commits.get(
                        qualified,
                        COMMITSEQ_ORIGIN,
                    )
                    watermark = watermarks.get(qualified)
                    if watermark is None or watermark <= commit:
                        continue
                    before_prefix = self._before_img_prefixes[
                        tracked.data_table.qualified
                    ]
                    raw_schema = self._extract_raw_schema(
                        session,
                        tracked,
                        before_prefix,
                    )
                    if self._schema_registry is None:  # pragma: no cover
                        return
                    schema_version = self._schema_registry.register_from_log(
                        tracked.data_table.qualified,
                        raw_schema,
                    )
                    yield from self._query_table_changes(
                        connection=connection,
                        tracked=tracked,
                        commitseq=commit,
                        watermark=watermark,
                        schema_version=schema_version,
                    )
                    self._mark.commits[qualified] = watermark
                sleep = min(
                    POLL_INTERVAL_SECONDS,
                    max(deadline - time.monotonic(), 0.0),
                )
                if sleep <= 0:
                    return
                time.sleep(sleep)
