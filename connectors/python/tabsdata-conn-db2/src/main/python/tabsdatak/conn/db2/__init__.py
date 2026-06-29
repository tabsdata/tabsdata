"""Db2 connector — public API.

Batch:

* :class:`Db2SrcConn` / :class:`Db2DestConn` -- per-instance Db2
  connections for :class:`Db2Src` / :class:`Db2Dest`.
  Attribute-identical but distinct classes so source and
  destination connections cannot be confused at the wire-format
  level.
* :class:`Db2Src` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`Db2Dest` -- destination: list of target table names;
  consumes one parquet slot per table.

CDC (change-data capture):

* :class:`Db2CdcStreamSrcConn` -- per-instance Db2 CDC connection.
  Sibling of the batch Db2 conns; expects the IBM SQL Replication
  apparatus (``ASN.IBMSNAP_*`` control tables + ``asncap``
  capture program) to be in place in the same database.
* :class:`Db2CdcStreamSrc` -- stream source that polls the IBM
  SQL Replication CD (change-data) tables and emits one
  tableframe-batch per capture flush. ``tables`` lists the CD
  table FQNs (not the source tables); the ingester resolves the
  source via ``ASN.IBMSNAP_REGISTER``.
* :class:`CommitSeqPosition` / :class:`TableCommitSeqPosition` /
  :class:`TimestampPosition` -- ``start_from`` primitives for
  resuming CD-table ingestion at a specific commit sequence.

URI and credential fields accept ``Secret`` references.
Cross-cutting cfg keys live in ``src_cfg`` / ``dest_cfg`` (chunk
size, read/write engine selection); CDC tuning lives in
``Db2CdcStreamSrc.src_cfg`` (buffer / trigger thresholds, polling
cadence, server-side fetch size).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# Private modules are imported as modules (not by symbol) so each
# call-site is explicit about reaching into private surface.
# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    BasicDictSpec,
    Conn,
    Dest,
    Secret,
    Src,
    StreamSrc,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _validation
from tabsdatak.conn.db2.error import Db2ErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
]
QueriesSpec: TypeAlias = list[str]
Db2TableNameSpec: TypeAlias = Annotated[
    str,
    _validation.DB2_TABLE_FQN_VALIDATOR,
]
Db2TableNamesSpec: TypeAlias = list[Db2TableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- Db2Src.src_cfg keys --------------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "db2.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Db2Dest.dest_cfg keys ------------------------------------------
DestCfgKey: TypeAlias = Literal[
    "db2.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_1, kw_only=True)
class Db2SrcConn(Conn):
    """Db2 connection used by :class:`Db2Src`.

    :param uri: SQLAlchemy-style URI
        (``db2+ibm_db://host:port/db``). Accepts a plain str,
        or ``Secret``.
    :param credentials: :class:`UserPassword` -- user + password
        spliced into the URI by the engine factory.
    :param entity_whitelist: Optional list of regex patterns; the
        runtime uses these to narrow which tables this conn
        contributes to discovery.
    :param conn_cfg: Optional driver-side config (forwarded as
        ``connect_args`` to SQLAlchemy).
    """

    __kind__ = "db2SrcConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


@_api.dataclass(Db2ErrorCode.DB2_1, kw_only=True)
class Db2DestConn(Conn):
    """Db2 connection used by :class:`Db2Dest`.

    Attribute-identical to :class:`Db2SrcConn` but a distinct class
    so source and destination connections cannot be confused at
    the wire-format level (the ``__kind__`` differs).
    """

    __kind__ = "db2DestConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


# --- Source ----------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_2, kw_only=True)
class Db2Src(Src):
    """Db2 source — one parquet slot per SELECT query.

    :param queries: List of SELECT statements; one slot per query.
    :param transactional: When ``True`` (default), run all queries
        inside one transactional session.
    :param schema_overrides: Optional per-query column dtype hints
        (same length as ``queries``).
    :param initial_values: Optional ``{bind_name: value}`` seed dict.
        Each name is bound into the query as a SQLAlchemy ``:name``
        parameter at execute time. Acts as the **first-run seed** for
        each bind name: at runtime, the plugin looks up
        ``ctx.get_attr(bind_name)`` per bind, and falls back to this
        field when the ctx scratch store has no entry for that name.
        User functions persist new values for the next run with
        ``ctx.set_attr(bind_name, new_value)``.
    :param src_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, readEngine, chunkSize).
    """

    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        if self.schema_overrides is not None and len(self.schema_overrides) != len(
            self.queries
        ):
            raise Db2ErrorCode.DB2_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_4, kw_only=True)
class Db2Dest(Dest):
    """Db2 destination — write one parquet slot per target table.

    :param tables: Target table names (FQN or bare); one parquet
        slot per table.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (truncate before insert).
    :param transactional: When ``True`` (default), wrap every table
        write in one transactional session.
    :param dest_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, writeEngine, chunkSize).
    """

    tables: Db2TableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC public primitives ------------------------------------------
#
# These types are db2-local rather than re-exported from
# ``conn.common.sql.cdc.typing`` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (``_cdc_plugin.py``) converts them onto the runner-side common
# representation when invoking the dialect-agnostic ``run_cdc`` loop.

# Relative position keywords: ``"head"`` rewinds to the oldest
# available row in each tracked CD (change-data) table; ``"tail"``
# skips to each CD table's current watermark.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@_api.dataclass(Db2ErrorCode.DB2_12, frozen=True, kw_only=True)
class CommitSeqPosition:
    """Start ingestion at a single global commit sequence applied
    to every tracked CD table.

    :param seq: Hex-string form of the binary 16-byte
        ``IBMSNAP_COMMITSEQ`` column (e.g.
        ``"00000000000000000000000000000042"``). All-zeros is the
        cold-start marker.
    """

    seq: str


@_api.dataclass(Db2ErrorCode.DB2_13, frozen=True, kw_only=True)
class TableCommitSeqPosition:
    """Start ingestion at per-table commit sequences -- useful when
    different CD tables need to resume from independent positions
    (e.g. after a partial re-snapshot).

    :param seqs: Mapping of qualified CD table names
        (``"CDSCHEMA.CDTABLE"``) to hex-string commit-sequence
        values. Tracked CD tables missing from this mapping start
        from :data:`COMMITSEQ_ORIGIN` (16 zero bytes).
    """

    seqs: dict[str, str]


@_api.dataclass(Db2ErrorCode.DB2_14, frozen=True, kw_only=True)
class TimestampPosition:
    """Start ingestion at the earliest commit sequence whose
    ``IBMSNAP_LOGMARKER`` is at or after ``ts``. The translation
    is a one-shot lookup in ``ASN.IBMSNAP_UOW``; once resolved the
    per-table marks advance independently.

    :param ts: Wall-clock cut-off; tz-aware datetimes are honoured,
        naive datetimes are interpreted in the Db2 server's
        timezone.
    """

    ts: datetime


Db2CdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    CommitSeqPosition,
    TableCommitSeqPosition,
    TimestampPosition,
]


# --- Db2CdcStreamSrc.src_cfg keys -----------------------------------
CdcSrcCfgKey: TypeAlias = Literal[
    "db2.cdc.blocking_timeout_seconds",
    "db2.cdc.fetch_size",
    "db2.cdc.logging_level",
    "tabsdata.cdc.buffer.max_rows",
    "tabsdata.cdc.buffer.max_bytes",
    "tabsdata.cdc.buffer.max_secs",
    "tabsdata.cdc.trigger.max_rows",
    "tabsdata.cdc.trigger.max_bytes",
    "tabsdata.cdc.trigger.max_secs",
    "tabsdata.cdc.poll_interval_seconds",
]

CdcSrcCfgSpec: TypeAlias = Annotated[
    Mapping[CdcSrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


@_api.dataclass(Db2ErrorCode.DB2_10, kw_only=True)
class Db2CdcStreamSrcConn(Conn):
    """Db2 CDC connection used by :class:`Db2CdcStreamSrc`.

    Mirrors :class:`Db2SrcConn` for the SQLAlchemy / catalog
    introspection side. SQL Replication does not require a
    distinct replica identifier on the connection: the CD tables
    + the ``ASN.IBMSNAP_*`` capture tables live in the same
    database that the source resides in, so a normal authenticated
    role with SELECT on the CD tables and ``ASN.IBMSNAP_REGISTER``
    is sufficient.

    Not a subclass of :class:`Db2SrcConn`: CDC and batch SQL have
    different operational guarantees (the capture / apply schema
    has to be in place, the connecting role typically needs
    different grants) and a divergent attribute set is expected
    over time.

    :param uri: SQLAlchemy-style URI; accepts ``str`` /
        ``Secret`` per :data:`UriSpec`.
    :param credentials: :class:`UserPassword`.
    :param entity_whitelist: Optional regex patterns narrowing
        discovery.
    :param conn_cfg: Optional driver-side config forwarded as
        ``connect_args``.
    """

    __kind__ = "db2CdcStreamSrcConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


@_api.dataclass(Db2ErrorCode.DB2_11, kw_only=True)
class Db2CdcStreamSrc(StreamSrc):
    """Db2 CDC stream source -- emits one tableframe-batch per
    capture-table flush across the tracked CD (change-data)
    tables.

    Db2 SQL Replication captures changes into shadow ``CD``
    tables; the ASN capture program (``asncap``) populates them
    in the background. ``tables`` here lists the CD tables (not
    the source tables); the ingester resolves the source via
    ``ASN.IBMSNAP_REGISTER`` and reads the IBMSNAP_* system
    columns to discover the operation / commit sequence /
    intent sequence / before-image prefix.

    :param tables: Qualified ``cd_schema.cd_table`` CD table names.
    :param start_from: ``"head"`` (every tracked CD table replayed
        from the origin), ``"tail"`` (each table's current
        watermark), a :class:`CommitSeqPosition` (single seq
        applied to every table), a :class:`TableCommitSeqPosition`
        (per-table seqs), or a :class:`TimestampPosition` (the
        max commit sequence whose IBMSNAP_LOGMARKER is at or
        before ``ts``).
    :param columns_format: Layout for the new (and old) columns
        payload on each emitted change row. See
        :data:`CdcColumnsFormatSpec`.
    :param flatten_columns: When ``True`` the new columns are
        inlined as top-level columns.
    :param src_cfg: Optional ``Mapping`` over the literal
        :data:`CdcSrcCfgKey` set -- buffer / trigger thresholds,
        polling cadence, server-side fetch size.
    """

    tables: list[Db2TableNameSpec]
    start_from: Db2CdcStartFromSpec
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
