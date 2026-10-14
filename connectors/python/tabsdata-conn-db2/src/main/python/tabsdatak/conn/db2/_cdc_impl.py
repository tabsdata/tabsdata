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
from datetime import datetime, tzinfo
from pathlib import Path
from typing import Any, Literal, TypeAlias, Union

import sqlalchemy as sa
import sqlalchemy.types as sa_types
from sqlalchemy import text
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import CLOB, LargeBinary, TypeEngine

from tabsdatak.conn.common._stream._offsets import TabsdataKeptOffsets

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import DEFAULT_POLL_MAX_SECS
from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._ingester import (
    CDC_OFFSET_STATE_KEY,
    CdcIngester,
)
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    InitialLoadSession,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._tz import parse_timezone, utc_instant
from tabsdatak.conn.common.sql.cdc.constants import (
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

# Db2 has no snapshot isolation. Cursor stability reads only committed data and
# holds no lock beyond the current row; stronger levels lock every row read or
# the whole table for the length of the load.
INITIAL_LOAD_ISOLATION_LEVEL = "CURSOR STABILITY"

CAPTURE_CATCH_UP_TIMEOUT_SECONDS = 3600

# IBMSNAP_COMMITSEQ is a log record identifier: an 8-byte log flush sequence
# followed by the 8-byte log sequence number that MON_GET_TRANSACTION_LOG
# reports as CURRENT_LSN.
COMMITSEQ_LSN_OFFSET = 8
COMMITSEQ_LSN_LENGTH = 8


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
    from ibm_db_sa.base import DBCLOB, XML

    _LARGE_OBJECT_TYPES = (LargeBinary, CLOB, DBCLOB, XML)
except ImportError:
    _LARGE_OBJECT_TYPES = (LargeBinary, CLOB)


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
            return parse_timezone(row[0])
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
    keyed by qualified CD-table name.

    A table nothing has been read from yet holds its registration's capture
    start (``CD_OLD_SYNCHPOINT``) rather than the zero origin: no CD row sits
    at or below it, so it reads the same, and it stays verifiable against
    what Capture records (see :func:`verify_positions`)."""

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
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict):
                raise TypeError("the offset is not a table map")
            commits = {table: bytes.fromhex(seq) for table, seq in payload.items()}
            if any(
                len(commitseq) != len(COMMITSEQ_ORIGIN)
                for commitseq in commits.values()
            ):
                raise ValueError("a commit sequence is not 16 bytes")
        except (TypeError, ValueError) as error:
            raise Db2ErrorCode.DB2_28.exception(cause=error, state=data)
        return cls(commits=commits)

    def copy(self) -> "CommitSeqOffset":
        return CommitSeqOffset(commits=self.commits)


@dataclass(frozen=True)
class RegistrationPositions:
    """What Capture records of one CD table's registration: its state, the
    log position it has captured from without a break (``CD_OLD_SYNCHPOINT``,
    moved up by a restart of the registration, a cold start or
    retention-limit pruning), and the lowest position the Apply
    subscriptions to it have read (``IBMSNAP_PRUNCNTL.SYNCHPOINT``), up to
    which normal pruning deletes its rows."""

    state: str
    state_info: str | None
    capture_start: bytes | None
    prune_bound: bytes | None
    splits_updates: bool = False

    @property
    def floor(self) -> bytes | None:
        """The lowest position every retained row sits above."""
        if self.capture_start is None:
            return None
        if self.prune_bound is None:
            return self.capture_start
        return max(self.capture_start, self.prune_bound)


def registration_positions(
    connection: Any, cd_tables: list[TableObject]
) -> dict[str, RegistrationPositions]:
    """Every one of `cd_tables`' registration positions, in one statement."""
    if not cd_tables:
        return {}
    matches = " or ".join(
        f"(r.cd_owner = '{cd_table.schema}' and r.cd_table = '{cd_table.table}')"
        for cd_table in cd_tables
    )
    rows = connection.execute(
        text(
            "select r.cd_owner, r.cd_table, r.state, r.state_info, "
            "r.cd_old_synchpoint, r.chg_upd_to_del_ins, (select min(p.synchpoint) "
            f"from {ASN_SCHEMA}.ibmsnap_pruncntl p "
            "where p.phys_change_owner = r.cd_owner "
            "and p.phys_change_table = r.cd_table) "
            f"from {ASN_SCHEMA}.ibmsnap_register r where {matches}"
        )
    ).fetchall()
    return {
        TableObject(schema=owner.strip(), table=table.strip()).qualified: (
            RegistrationPositions(
                state=(state or "").strip(),
                state_info=state_info.strip() if state_info else None,
                capture_start=capture_start,
                prune_bound=prune_bound,
                splits_updates=(splits or "").strip().upper() == "Y",
            )
        )
        for owner, table, state, state_info, capture_start, splits, prune_bound in rows
    }


def verify_positions(
    positions: dict[str, RegistrationPositions],
    commits: dict[str, bytes],
    cd_tables: list[TableObject],
) -> None:
    """Fail unless every row after each table's position is still there.

    Capture only removes rows from the oldest up, and only at or below one
    of the two positions it records: the capture start of the registration
    or the prune bound of its subscriptions. With both at or below a
    table's position, nothing after it can have been removed; with either
    past it, rows after it may have been, so the stream cannot continue."""
    for cd_table in cd_tables:
        qualified = cd_table.qualified
        position = commits.get(qualified)
        if position is None:
            raise Db2ErrorCode.DB2_33.exception(table=qualified)
        registration = positions.get(qualified)
        if registration is None:
            raise Db2ErrorCode.DB2_29.exception(table=qualified)
        if registration.state != "A":
            raise Db2ErrorCode.DB2_30.exception(
                table=qualified,
                state=registration.state,
                info=registration.state_info or "none",
            )
        if registration.capture_start is None or registration.capture_start > position:
            raise Db2ErrorCode.DB2_31.exception(
                table=qualified,
                start=registration.capture_start.hex()
                if registration.capture_start is not None
                else "none",
                position=position.hex(),
            )
        if registration.prune_bound is not None and registration.prune_bound > position:
            raise Db2ErrorCode.DB2_32.exception(
                table=qualified,
                bound=registration.prune_bound.hex(),
                position=position.hex(),
            )


def commitseq_lsn(commitseq: bytes) -> int:
    return int.from_bytes(
        commitseq[COMMITSEQ_LSN_OFFSET : COMMITSEQ_LSN_OFFSET + COMMITSEQ_LSN_LENGTH],
        byteorder="big",
    )


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class SqlReplicaInitialLoadSession(InitialLoadSession):
    """Initial load read under cursor stability and pinned to the database
    log position, without locking any table.

    The log position is read before the rows; once they are read, the session
    waits for the Capture program to pass it and resumes each CD table from
    its last commit before it. The highest commit sequence Capture has copied
    would lag the log, so commits not yet copied would be both loaded and
    replayed.

    Db2 has no snapshot isolation, so a change committed while the rows are
    read may be both loaded and replayed. No change is ever lost.
    """

    def __init__(self, *, engine: Any, server_tz: tzinfo | None):
        super().__init__()
        self._engine = engine
        self._server_tz = server_tz
        self._offset: CommitSeqOffset | None = None
        self._window_end: CommitSeqOffset | None = None
        self._tracked_tables: dict[str, TrackedTable] = {}

    @property
    def engine(self) -> Any:
        return self._engine

    @property
    def offset(self) -> CdcOffset:
        if self._offset is None:
            raise Db2ErrorCode.DB2_25.exception()
        return self._offset

    @property
    def window_end(self) -> CdcOffset | None:
        if self._window_end is None:
            raise Db2ErrorCode.DB2_25.exception()
        return self._window_end

    def _current_time(self, connection: Any) -> datetime:
        now: datetime = connection.execute(
            text("select current timestamp from sysibm.sysdummy1")
        ).scalar()
        return utc_instant(now, self._server_tz)

    def tables(
        self, tracked_tables: dict[str, TrackedTable]
    ) -> Iterator[tuple[TrackedTable, Any]]:
        # Pinned even with nothing to yield, so the offset is established
        # whatever the caller iterates.
        self._check_currently_committed()
        with self._engine.connect().execution_options(
            isolation_level=INITIAL_LOAD_ISOLATION_LEVEL
        ) as connection:
            self._timestamp = self._current_time(connection)
            lsn = self._current_lsn(connection)
            logger.info("Initial load snapshot starts at log position %x", lsn)
            for tracked_table in tracked_tables.values():
                yield tracked_table, connection
            connection.commit()
            self._wait_for_capture(connection, lsn)
            self._pin(connection, tracked_tables, lsn)
            # A commit at or before this position may be in what the load
            # read, so the window ends at it inclusively: one more change in
            # the window is deduplicated, one fewer would be replayed.
            end = self._current_lsn(connection)
            logger.info("Initial load snapshot ends at log position %x", end)
            self._wait_for_capture(connection, end)
            _, commits = self._positions_before(
                connection, tracked_tables, end, inclusive=True
            )
            self._window_end = CommitSeqOffset(commits=commits)
            logger.info(
                "Initial load window ends for each CD table at: %s",
                {table: commitseq.hex() for table, commitseq in commits.items()},
            )

    def _check_currently_committed(self) -> None:
        # Without currently committed semantics the read still sees only
        # committed data, but it waits behind writers' uncommitted changes.
        try:
            with self._engine.connect() as connection:
                value = connection.execute(
                    text("select value from sysibmadm.dbcfg where name = 'cur_commit'")
                ).scalar()
        except sa.exc.DBAPIError as error:
            logger.warning(
                "Could not read the cur_commit database setting, so the initial "
                "load may wait behind uncommitted changes: %s",
                error,
            )
            return
        setting = str(value).strip().upper() if value is not None else None
        if setting != "ON":
            logger.warning(
                "The cur_commit database setting is %r rather than 'ON', so the "
                "initial load waits behind uncommitted changes instead of reading "
                "the last committed version of each row",
                setting,
            )

    def _current_lsn(self, connection: Any) -> int:
        try:
            lsn = connection.execute(
                text("select current_lsn from table(mon_get_transaction_log(-1))")
            ).scalar()
        except sa.exc.DBAPIError as error:
            raise Db2ErrorCode.DB2_27.exception(cause=error, reason=str(error))
        return int(lsn)

    def _wait_for_capture(self, connection: Any, lsn: int) -> None:
        # Capture reads the log in order, so once either position reaches the
        # pin every earlier commit is in the CD tables. The commit position
        # stays still while nothing commits and the in-flight one stays still
        # while a transaction is open, so either one is enough.
        deadline = time.monotonic() + CAPTURE_CATCH_UP_TIMEOUT_SECONDS
        waiting = False
        while True:
            row = connection.execute(
                text(
                    "select max_commitseq, min_inflightseq "
                    f"from {ASN_SCHEMA}.IBMSNAP_RESTART"
                )
            ).fetchone()
            connection.commit()
            if row is not None and any(
                position is not None and commitseq_lsn(position) >= lsn
                for position in row
            ):
                return
            if time.monotonic() >= deadline:
                raise Db2ErrorCode.DB2_26.exception(
                    lsn=f"{lsn:x}",
                    timeout=CAPTURE_CATCH_UP_TIMEOUT_SECONDS,
                )
            if not waiting:
                logger.info(
                    "Waiting for the Capture program to reach log position %x",
                    lsn,
                )
                waiting = True
            time.sleep(POLL_INTERVAL_SECONDS)

    def _pin(
        self,
        connection: Any,
        tracked_tables: dict[str, TrackedTable],
        lsn: int,
    ) -> None:
        positions, commits = self._positions_before(
            connection, tracked_tables, lsn, inclusive=False
        )
        for tracked in tracked_tables.values():
            qualified = tracked.cdc_table.qualified
            if positions[qualified].splits_updates:
                raise Db2ErrorCode.DB2_35.exception(table=qualified)
            # Capture must have been capturing the table since before the
            # load read it, or the changes in between are in neither.
            start = positions[qualified].capture_start
            if commitseq_lsn(start) >= lsn:
                raise Db2ErrorCode.DB2_34.exception(
                    table=qualified,
                    start=start.hex(),
                    lsn=f"{lsn:x}",
                )
        self._offset = CommitSeqOffset(commits=commits)
        logger.info(
            "Initial load resumes each CD table from its last commit before log "
            "position %x: %s",
            lsn,
            {table: commitseq.hex() for table, commitseq in commits.items()},
        )

    def _positions_before(
        self,
        connection: Any,
        tracked_tables: dict[str, TrackedTable],
        lsn: int,
        *,
        inclusive: bool,
    ) -> tuple[dict[str, RegistrationPositions], dict[str, bytes]]:
        """Each CD table's registration positions, and its last commit before
        `lsn` (or at it, when `inclusive`), verified to be servable.

        Within one member the log flush sequence grows with the log sequence
        number, so the rows before the position are exactly those at or below
        the returned commit sequence. A table with none is at its
        registration's floor, since every row it holds is after the position.
        """
        commits: dict[str, bytes] = {}
        cd_tables = [tracked.cdc_table for tracked in tracked_tables.values()]
        positions = registration_positions(connection, cd_tables)
        if cd_tables:
            comparison = "<=" if inclusive else "<"
            blocks = [
                f"select {index} as idx, max({IBMSNAP_COMMITSEQ.quoted}) as mcs "
                f'from "{cd_table.schema}"."{cd_table.table}" '
                f"where substr({IBMSNAP_COMMITSEQ.quoted}, "
                f"{COMMITSEQ_LSN_OFFSET + 1}, {COMMITSEQ_LSN_LENGTH}) "
                f"{comparison} :bound"
                for index, cd_table in enumerate(cd_tables)
            ]
            rows = connection.execute(
                text(" union all ".join(blocks)),
                {"bound": lsn.to_bytes(COMMITSEQ_LSN_LENGTH, byteorder="big")},
            ).fetchall()
            before = {index: commitseq for index, commitseq in rows}
            for index, cd_table in enumerate(cd_tables):
                registration = positions.get(cd_table.qualified)
                floor = registration.floor if registration else None
                commitseq = before.get(index) or floor
                if commitseq is not None:
                    commits[cd_table.qualified] = commitseq
        verify_positions(positions, commits, cd_tables)
        connection.commit()
        return positions, commits

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
        poll_max_secs: int = DEFAULT_POLL_MAX_SECS,
        fetch_size: int = DEFAULT_FETCH_SIZE,
    ):
        super().__init__(tables=tables)
        self.offsets = TabsdataKeptOffsets(
            key=CDC_OFFSET_STATE_KEY,
            read=CommitSeqOffset.read,
        )
        self._conn = conn
        self._start_from = start_from
        self._poll_max_secs = poll_max_secs
        self._fetch_size = fetch_size
        self._mark: CommitSeqOffset | None = None
        self._base_offset: CommitSeqOffset | None = None
        self._bound: CommitSeqOffset | None = None
        self._bound_reached = False
        # Per quiet CD table: the position of the last row read from it, and
        # the position it has been moved on to, with no row in between.
        self._quiet: dict[str, tuple[bytes, bytes]] = {}
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

    def open_session(self, work_dir: Path) -> Any:
        engine = self._build_engine()
        self._server_tz = self.dialect.server_timezone(engine)
        logger.info("Db2 server timezone: %s", self._server_tz)
        return engine

    def open_initial_load(self) -> SqlReplicaInitialLoadSession:
        return SqlReplicaInitialLoadSession(
            engine=self._build_engine(), server_tz=self._server_tz
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

        return sqlalchemy_utils.create_engine(
            self._resolve_url(), connect_args=self.connect_args
        )

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

    # ------------------------------------------------------------------
    # init_reader -- translate start_from to the per-table commit map
    # ------------------------------------------------------------------

    def _init_reader(self, session: Any) -> None:
        with session.connect() as connection:
            positions = registration_positions(connection, self._cd_tables())
        if self._mark is None:
            self._mark = CommitSeqOffset(
                commits=self._start_commits(session, positions)
            )
        verify_positions(positions, self._mark.commits, self._cd_tables())
        self._base_offset = self._mark.copy()

    def _cd_tables(self) -> list[TableObject]:
        return [tracked.cdc_table for tracked in self._tracked_tables.values()]

    def _start_commits(
        self, session: Any, positions: dict[str, RegistrationPositions]
    ) -> dict[str, bytes]:
        def floor(qualified: str) -> bytes:
            registration = positions.get(qualified)
            return (registration.floor if registration else None) or COMMITSEQ_ORIGIN

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
                    qualified = tracked.cdc_table.qualified
                    commits[qualified] = floor(qualified)
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
                    commits[qualified] = max(
                        watermarks.get(qualified, globalseq), floor(qualified)
                    )
            case _:
                raise Db2ErrorCode.DB2_15.exception(
                    start_from=self._start_from,
                )
        return commits

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
            commit_ts = utc_instant(row_map[IBMSNAP_LOGMARKER.sa_name], self._server_tz)
            new_values = {col.name: row_map.get(col.sa_name) for col in tracked.columns}
            if op == CdcOperation.UPDATE:
                old_values = {}
                for column in tracked.columns:
                    if column.sa_oname and column.sa_oname in row_map:
                        old_values[column.name] = row_map[column.sa_oname]
                    else:
                        old_values[column.name] = None
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

    def bound_stream(self, end: CdcOffset) -> None:
        if not isinstance(end, CommitSeqOffset):
            raise Db2ErrorCode.DB2_21.exception(actual=type(end).__name__)
        self._bound = end
        self._bound_reached = False

    def stream_bound_reached(self) -> bool:
        return self._bound_reached

    @staticmethod
    def _captured_position(connection: Any) -> bytes | None:
        """How far Capture has captured and committed to the CD tables
        (``IBMSNAP_RESTART.MAX_COMMITSEQ``), read before the tables are: every
        CD row at or below it is then visible to the read that follows."""
        position = connection.execute(
            text(f"select max_commitseq from {ASN_SCHEMA}.IBMSNAP_RESTART")
        ).scalar()
        return bytes(position) if position is not None else None

    def _advance_quiet_tables(self, captured: bytes | None, read: set[str]) -> None:
        """Move each table that had no row to read on to Capture's position.
        Every row of it up to that position was committed before the read,
        which found none, and the check after the read shows none of them was
        pruned: so there is none, and the table can stand there. Otherwise a
        quiet table would stand still while Apply subscriptions let Capture
        prune past it, and fail the stream although it missed nothing."""
        if captured is None:
            return
        for cd_table in self._cd_tables():
            qualified = cd_table.qualified
            if qualified in read:
                self._quiet.pop(qualified, None)
                continue
            position = self._mark.commits.get(qualified, COMMITSEQ_ORIGIN)
            if captured <= position:
                continue
            last_row = self._quiet.get(qualified, (position, position))[0]
            self._quiet[qualified] = (last_row, captured)
            self._mark.commits[qualified] = captured

    def staged_offset(self, offset: CdcOffset) -> CdcOffset:
        """The staged offset over every tracked table: one the stage has no
        row of stands where the stream started it, and each quiet table at
        the position it was moved on to, once the stage covers the last row
        read from it: a stage short of that row cannot skip it."""
        commits = dict(self._base_offset.commits) if self._base_offset else {}
        commits.update(offset.commits)
        for table, (last_row, moved_to) in self._quiet.items():
            staged = commits.get(table)
            if staged is not None and staged >= last_row and moved_to > staged:
                commits[table] = moved_to
        return CommitSeqOffset(commits=commits)

    def poll_changes(self, session: Any) -> Iterable[CdcChange]:
        # Lazy init_reader on the first poll -- collect_tables
        # populates ``_tracked_tables`` first.
        if self._base_offset is None:
            self._init_reader(session)
        bound = self._bound
        qualified: str | None = None
        deadline = time.monotonic() + self._poll_max_secs
        with session.connect() as connection:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                captured = self._captured_position(connection)
                watermarks = self._query_table_watermarks(connection)
                read_from = dict(self._mark.commits)
                read: set[str] = set()
                for tracked in self._tracked_tables.values():
                    qualified = tracked.cdc_table.qualified
                    commit = self._mark.commits.get(
                        qualified,
                        COMMITSEQ_ORIGIN,
                    )
                    watermark = watermarks.get(qualified)
                    end = bound.commits.get(qualified) if bound else None
                    if watermark is not None and end is not None and watermark > end:
                        watermark = end
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
                    read.add(qualified)
                # Checked after the read, against where it started: Capture's
                # positions only move up, so with both still at or below it
                # nothing the read needed was pruned while it ran.
                verify_positions(
                    registration_positions(connection, self._cd_tables()),
                    read_from,
                    self._cd_tables(),
                )
                if bound is not None:
                    # Capture has passed the bound, so one pass read every
                    # row up to it; a table with none there stands at it too.
                    for table, end in bound.commits.items():
                        self._mark.commits[table] = max(
                            self._mark.commits.get(table, COMMITSEQ_ORIGIN), end
                        )
                    # Everything up to the bound is staged with the load, so
                    # the stream starts there rather than at the load's pin.
                    self._base_offset = self._mark.copy()
                    self._bound = None
                    self._bound_reached = True
                    return
                self._advance_quiet_tables(captured, read)
                sleep = min(
                    POLL_INTERVAL_SECONDS,
                    max(deadline - time.monotonic(), 0.0),
                )
                if sleep <= 0:
                    return
                time.sleep(sleep)
