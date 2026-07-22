#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 connector -- public API.

Imported from `tabsdatak.conn.db2`. Batch classes:

* `Db2SrcConn` / `Db2DestConn` -- the connection: a SQLAlchemy URI
  plus `UserPassword` credentials. Attribute-identical but distinct
  classes so source and destination connections cannot be confused at
  the wire-format level.
* `Db2Src` -- the `@publisher` source: a list of SELECT `queries`,
  one parquet slot per query.
* `Db2Dest` -- the `@subscriber` destination: a list of target
  `tables`, one parquet slot per table.

CDC (change-data capture) classes:

* `Db2CdcStreamSrcConn` -- the CDC connection. Expects the IBM SQL
  Replication apparatus (`ASN.IBMSNAP_*` control tables + the
  `asncap` capture program) in the same database.
* `Db2CdcStreamSrc` -- the `@stream_publisher` source; polls the CD
  (change-data) tables and emits one tableframe-batch per capture flush.
* `CommitSeqPosition` / `TableCommitSeqPosition` /
  `TimestampPosition` -- `start_from` primitives for resuming
  CD-table ingestion at a specific commit sequence.

The connection (with credentials) is registered on a collection and
resolved from the function's collection at run time -- it is never passed
to a decorator. A flow function only references the source / destination.

Examples:
    .. code-block:: python

        @publisher(source=Db2Src(queries=["SELECT * FROM wld_city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=Db2Dest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(cities: TableFrameSpec) -> TableFrameSpec:
            return cities
A `@stream_publisher` receives the flushed batch as a list of frames
and must NOT declare a `ctx` parameter.

Examples:
    .. code-block:: python

        @stream_publisher(source=Db2CdcStreamSrc(tables=["CDC.CITY"],
                                                 start_from="tail"),
                          flush_interval_mins=2, output_tables=["cdc_raw"])
        def stream(rows: TableFramesSpec) -> TableFrameSpec:
            return concat(rows)
URI and credential fields accept `Secret` references. Cross-cutting
cfg keys live in `src_cfg` / `dest_cfg` (chunk size, read/write
engine selection); CDC tuning lives in `Db2CdcStreamSrc.src_cfg`
(buffer / trigger thresholds, polling cadence, server-side fetch size).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

# Private modules are imported as modules (not by symbol) so each
# call-site is explicit about reaching into private surface.
# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    Src,
    StreamSrc,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _validation
from tabsdatak.conn.db2.error import Db2ErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for Db2 connections: `UserPassword` (user +
password), spliced into the URI by the engine factory."""

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.URI_PATTERN)
]
"""A SQLAlchemy-style Db2 URI, e.g. `db2+ibm_db://host:port/db`. The
scheme must be `db2` or `ibm_db_sa` (an optional `+driver` is
allowed). Accepts a plain string or a `Secret`."""

QueriesSpec: TypeAlias = list[str]
"""A non-empty, ordered list of SELECT statements -- one output slot per
query."""

Db2TableNameSpec: TypeAlias = Annotated[
    str,
    _validation.DB2_TABLE_FQN_VALIDATOR,
]
"""A Db2 table name, either `table` or `schema.table` (at most two
dot-separated parts). Parts may be double-quoted, using the doubled
double-quote convention to embed a literal quote."""

Db2TableNamesSpec: TypeAlias = list[Db2TableNameSpec]
"""A list of Db2 table names (see `Db2TableNameSpec`)."""

IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
"""Write disposition when the target table exists: `"append"` adds
rows; `"replace"` truncates before insert."""


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
    """Db2 connection used by `Db2Src`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        uri: SQLAlchemy-style Db2 URI (`db2+ibm_db://host:port/db`).
            Accepts a plain string or a `Secret`.
        credentials: `UserPassword` -- user + password spliced into
            the URI by the engine factory.
        entity_whitelist: Optional list of regex patterns narrowing
            which tables this connection contributes to discovery.
            `None` (the default) means no filtering.
        conn_cfg: Optional driver-side config forwarded to SQLAlchemy as
            `connect_args`.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(Db2ErrorCode.DB2_1, kw_only=True)
class Db2DestConn(Conn):
    """Db2 connection used by `Db2Dest`.

    :categories: destination

    Attribute-identical to `Db2SrcConn` but a distinct class so source
    and destination connections cannot be confused at the wire-format
    level (the `__kind__` differs). See `Db2SrcConn` for the field
    meanings.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_2, kw_only=True)
class Db2Src(Src):
    """Db2 source for `@publisher` -- one parquet slot per query.

    :categories: source

    Args:
        queries: SELECT statements; one output slot per query, in order.
        transactional: When `True` (default), run all queries inside
            one transactional session.
        schema_overrides: Optional per-query column dtype hints. When
            given it must have exactly one entry per query (same length
            and order as `queries`); each entry is a
            `{column: DataType}` map or `None`.
        initial_values: Optional `{bind_name: value}` seed dict. Each
            name is bound into the query as a SQLAlchemy `:var`
            parameter at execute time, and acts as the first-run seed:
            the plugin reads `ctx.get_attr(bind_name)` per bind and
            falls back to this value when the ctx store has no entry.
            Persist the next value with `ctx.set_attr(bind_name, v)`.
        src_cfg: Optional config mapping over the allowed keys only
            (`db2.logging_level`, `tabsdata.sql.sql.readEngine`,
            `tabsdata.sql.chunkSize`).

    Examples:
        Publish two Db2 tables, one output slot per query in order:

        .. code-block:: python

            @publisher(
                source=Db2Src(
                    queries=[
                        "SELECT * FROM wld_country",
                        "SELECT * FROM wld_city",
                    ],
                ),
                output_tables=["d2_country", "d2_city"],
            )
            def ingest(country: TableFrameSpec, city: TableFrameSpec):
                return country, city

        Incremental read: an `initial_values` bind seeds the `:wm`
        watermark on the first run, then `ctx` advances it each run.
        Db2 folds unquoted identifiers to uppercase, so the column
        is `ID`:

        .. code-block:: python

            @publisher(
                source=Db2Src(
                    queries=[
                        "SELECT * FROM wld_city WHERE id > :wm "
                        "ORDER BY id FETCH FIRST 10 ROWS ONLY"
                    ],
                    initial_values={"wm": 0},
                ),
                output_tables=["d2_city_inc"],
            )
            def ingest_inc(city: TableFrameSpec, ctx: TrxCtx):
                if city is not None:
                    row = city.max_for("ID")
                    if row is not None and row["ID"] is not None:
                        ctx.set_attr("wm", int(row["ID"]))
                return city
    """

    # devguide-ref: trx context state (initial_values seed / cursor resume)
    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Check `schema_overrides` has one entry per query.

        Called by the framework; raises `Db2ValidateException` when the
        lengths differ.
        """
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
    """Db2 destination for `@subscriber` -- one target table per slot.

    :categories: destination

    Args:
        tables: Target table names, qualified or bare (see
            `Db2TableNameSpec`); one input slot per table, in order.
        if_table_exists: `"append"` (default) adds rows; `"replace"`
            truncates the table before inserting.
        transactional: When `True` (default), wrap every table write
            in one transactional session.
        dest_cfg: Optional config mapping over the allowed keys only
            (`db2.logging_level`, `tabsdata.sql.sql.writeEngine`,
            `tabsdata.sql.chunkSize`).
    """

    tables: Db2TableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC public primitives ------------------------------------------
#
# These types are db2-local rather than re-exported from
# `conn.common.sql.cdc.typing` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (`_cdc_plugin.py`) converts them onto the runner-side common
# representation when invoking the dialect-agnostic `run_cdc` loop.

# Relative position keywords: `"head"` rewinds to the oldest
# available row in each tracked CD (change-data) table; `"tail"`
# skips to each CD table's current watermark.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]
"""Relative CDC start position: `"head"` rewinds to the oldest row
still retained in each tracked CD table; `"tail"` skips to each CD
table's current watermark (only later changes are emitted)."""

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]
"""Layout of the change-values payload on each emitted row: one of
`"columns"`, `"struct"` or `"map"`."""


@_api.dataclass(Db2ErrorCode.DB2_12, frozen=True, kw_only=True)
class CommitSeqPosition:
    """CDC start position: one global commit sequence for every table.

    :categories: source

    A `start_from` value applying a single commit sequence to every
    tracked CD table.

    Args:
        seq: Hex-string form of the binary 16-byte `IBMSNAP_COMMITSEQ`
            column (e.g. `"000...0042"`). All zeros is the cold-start
            marker.
    """

    seq: str


@_api.dataclass(Db2ErrorCode.DB2_13, frozen=True, kw_only=True)
class TableCommitSeqPosition:
    """CDC start position: independent commit sequence per table.

    :categories: source

    A `start_from` value giving each CD table its own resume point --
    useful after a partial re-snapshot.

    Args:
        seqs: Mapping of qualified CD table names (`"CDSCHEMA.CDTABLE"`)
            to hex-string commit-sequence values. Tracked CD tables
            missing from the mapping start from the origin (16 zero
            bytes).
    """

    seqs: dict[str, str]


@_api.dataclass(Db2ErrorCode.DB2_14, frozen=True, kw_only=True)
class TimestampPosition:
    """CDC start position given as a wall-clock timestamp.

    :categories: source

    A `start_from` value resolved once to the earliest commit sequence
    whose `IBMSNAP_LOGMARKER` is at or after `ts` (a lookup in
    `ASN.IBMSNAP_UOW`); per-table marks then advance independently.

    Args:
        ts: Wall-clock cut-off. Tz-aware datetimes are honoured; naive
            datetimes are interpreted in the Db2 server's timezone.
    """

    ts: datetime


Db2CdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    CommitSeqPosition,
    TableCommitSeqPosition,
    TimestampPosition,
]
"""Where a Db2 CDC stream begins: a `"head"` / `"tail"` keyword, a
`CommitSeqPosition`, a `TableCommitSeqPosition` or a
`TimestampPosition`."""


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
    """Db2 CDC connection used by `Db2CdcStreamSrc`.

    :categories: source

    Mirrors `Db2SrcConn` for the SQLAlchemy / catalog introspection
    side. SQL Replication needs no distinct replica identifier on the
    connection: the CD tables and the `ASN.IBMSNAP_*` capture tables
    live in the same database as the source, so an authenticated role
    with SELECT on the CD tables and `ASN.IBMSNAP_REGISTER` suffices.
    Not a subclass of `Db2SrcConn` -- CDC and batch SQL have different
    operational guarantees (the capture schema must be in place, the
    connecting role typically needs different grants).

    Args:
        uri: SQLAlchemy-style Db2 URI; accepts a plain string or a
            `Secret` (see `UriSpec`).
        credentials: `UserPassword` credentials.
        entity_whitelist: Optional regex patterns narrowing discovery;
            `None` (default) means no filtering.
        conn_cfg: Optional driver-side config forwarded as
            `connect_args`.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(Db2ErrorCode.DB2_11, kw_only=True)
class Db2CdcStreamSrc(StreamSrc):
    """Db2 CDC stream source for `@stream_publisher`.

    :categories: source

    A streaming change-data-capture source: it emits one
    tableframe-batch per capture-table flush across the tracked CD
    (change-data) tables. Db2 SQL Replication captures changes into
    shadow CD tables that the ASN capture program (`asncap`) populates
    in the background. `tables` lists the CD tables (not the source
    tables); the ingester resolves the source via
    `ASN.IBMSNAP_REGISTER`. The decorated `@stream_publisher`
    function receives the flushed batch as a list of frames and must NOT
    declare a `ctx` parameter.

    Args:
        tables: Qualified `cd_schema.cd_table` CD table names.
        start_from: Where to begin -- `"head"` (replay each tracked CD
            table from the origin), `"tail"` (each table's current
            watermark), a `CommitSeqPosition`, a
            `TableCommitSeqPosition` or a `TimestampPosition`.
        columns_format: Layout for the new (and old) columns payload on
            each change row -- `"columns"` (default), `"struct"` or
            `"map"`.
        flatten_columns: When `True` (default), the new columns are
            inlined as top-level columns.
        src_cfg: Optional config mapping over the allowed CDC keys only
            -- buffer / trigger thresholds, polling cadence, server-side
            fetch size.
    """

    # devguide-ref: CDC cursor / resume (start_from, offset persistence)
    tables: list[Db2TableNameSpec]
    start_from: Db2CdcStartFromSpec
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
