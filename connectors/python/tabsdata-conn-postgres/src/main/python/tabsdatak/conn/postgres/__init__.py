#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres connector -- public API.

Imported from `tabsdatak.conn.postgres`. Batch classes:

* `PostgresSrcConn` / `PostgresDestConn` -- the connection: a
  SQLAlchemy URI plus `UserPassword` credentials. Attribute-identical
  but distinct classes, one for source and one for destination
  connections.
* `PostgresSrc` -- the `@publisher` source: a list of SELECT
  `queries`, one output slot per query.
* `PostgresDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table.

CDC (change-data capture) classes:

* `PostgresCdcStreamSrcConn` -- the CDC connection; the connecting
  role must hold the `REPLICATION` attribute.
* `PostgresCdcStreamSrc` -- the `@stream_publisher` source; emits
  one tableframe-batch per WAL flush across the tracked tables, attached
  to a logical replication slot named on the source.
* `LsnPosition` -- `start_from` primitive for resuming logical
  replication at a specific WAL LSN.

The connection (with credentials) is registered on a collection and
resolved from the function's collection at run time -- it is never passed
to a decorator. A flow function only references the source / destination.

Examples:
    .. code-block:: python

        @publisher(source=PostgresSrc(queries=["SELECT * FROM city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=PostgresDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(cities: TableFrameSpec) -> TableFrameSpec:
            return cities
A `@stream_publisher` receives the flushed batch as a list of frames
and must NOT declare a `ctx` parameter.

Examples:
    .. code-block:: python

        @stream_publisher(
            source=PostgresCdcStreamSrc(tables=["public.city"],
                                        replication_slot="td_slot",
                                        start_from="tail"),
            flush_interval_mins=2, output_tables=["cdc_raw"])
        def stream(rows: TableFramesSpec) -> TableFrameSpec:
            return concat(rows)
URI and credential fields accept `Secret` references. Cross-cutting
cfg keys live in `src_cfg` / `dest_cfg` (chunk size, read/write
engine selection); CDC tuning lives in `PostgresCdcStreamSrc.src_cfg`
(buffer / trigger thresholds, polling cadence, wal2json format version).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import model_validator

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
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common._stream import _floor
from tabsdatak.conn.common.sql.cdc.typing import validate_start
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _validation
from tabsdatak.conn.postgres.error import PostgresErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for Postgres connections: `UserPassword` (user
+ password), spliced into the URI by the engine factory."""

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.URI_VALIDATION,
]
"""A SQLAlchemy-style Postgres URI, e.g.
`postgres+psycopg2://host:port/db`. The scheme must be `postgres` or
`postgresql` (an optional `+driver` is allowed). Accepts a plain
string or a `Secret`."""

QueriesSpec: TypeAlias = list[str]
"""A non-empty, ordered list of SELECT statements -- one output slot per
query."""

PostgresTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.POSTGRES_TABLE_FQN_VALIDATION,
]
"""A Postgres table name -- `table`, `schema.table` or
`database.schema.table` (at most three dot-separated parts). Parts may
be double-quoted, using the doubled double-quote convention to embed a
literal quote."""

PostgresTableNamesSpec: TypeAlias = list[PostgresTableNameSpec]
"""A list of Postgres table names (see `PostgresTableNameSpec`)."""

IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
"""Write disposition when the target table exists: `"append"` adds
rows; `"replace"` truncates before insert."""


# --- PostgresSrc.src_cfg keys ---------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "postgres.logging_level",
    "tabsdata.sql.read_engine",
    "tabsdata.sql.chunk_size",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- PostgresDest.dest_cfg keys -------------------------------------
DestCfgKey: TypeAlias = Literal[
    "postgres.logging_level",
    "tabsdata.sql.write_engine",
    "tabsdata.sql.chunk_size",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_1, kw_only=True)
class PostgresSrcConn(Conn):
    """Postgres connection used by `PostgresSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        uri: SQLAlchemy-style Postgres URI
            (`postgres+psycopg2://host:port/db`). Accepts a plain
            string or a `Secret`.
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


@_api.dataclass(PostgresErrorCode.POSTGRES_1, kw_only=True)
class PostgresDestConn(Conn):
    """Postgres connection used by `PostgresDest`.

    :categories: destination

    Attribute-identical to `PostgresSrcConn` but a distinct class;
    source and destination connections are separate types and cannot be
    used in place of one another. See `PostgresSrcConn` for the field
    meanings.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_2, kw_only=True)
class PostgresSrc(Src):
    """Postgres source for `@publisher` -- one output slot per query.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.sql.query`, the text of the query that
    produced the row, and `@td.sql.initial_values`, the JSON
    serialization of the bind values that query ran with (an empty JSON
    object when it has no binds). Setting the `src_cfg` key
    `tabsdata.src_metadata.drop` to `True` leaves them off.

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
            (`postgres.logging_level`, `tabsdata.sql.read_engine`,
            `tabsdata.sql.chunk_size`, `tabsdata.src_metadata.drop`).

    Examples:
        Publish multiple Postgres tables, one output slot per query in order:

        .. code-block:: python

            @publisher(
                source=PostgresSrc(
                    queries=[
                        "SELECT * FROM wld_country",
                        "SELECT * FROM wld_city",
                    ],
                ),
                output_tables=["pg_country", "pg_city"],
            )
            def ingest(country: TableFrameSpec, city: TableFrameSpec):
                return country, city

        Incremental read: an `initial_values` bind seeds the `:wm`
        watermark on the first run, then `ctx` advances it each run:

        .. code-block:: python

            @publisher(
                source=PostgresSrc(
                    queries=[
                        "SELECT * FROM wld_city WHERE id > :wm ORDER BY id LIMIT 10",
                    ],
                    initial_values={"wm": 0},
                ),
                output_tables=["pg_city_inc"],
            )
            def ingest_inc(city: TableFrameSpec, ctx: TrxCtx):
                if city is not None:
                    row = city.max_for("id")
                    if row is not None and row["id"] is not None:
                        ctx.set_attr("wm", int(row["id"]))
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

        Called by the framework; raises `PostgresValidateException`
        when the lengths differ.
        """
        if self.schema_overrides is not None and len(self.schema_overrides) != len(
            self.queries
        ):
            raise PostgresErrorCode.POSTGRES_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_4, kw_only=True)
class PostgresDest(Dest):
    """Postgres destination for `@subscriber` -- one target table per slot.

    :categories: destination

    Args:
        tables: Target table names, qualified or bare (see
            `PostgresTableNameSpec`); one input slot per table, in
            order.
        if_table_exists: `"append"` (default) adds rows; `"replace"`
            truncates the table before inserting.
        transactional: When `True` (default), wrap every table write
            in one transactional session.
        dest_cfg: Optional config mapping over the allowed keys only
            (`postgres.logging_level`,
            `tabsdata.sql.write_engine`, `tabsdata.sql.chunk_size`).

    Note:
        Column types are picked only when the target table is created (with
        `"replace"`, or `"append"` to a missing table); writing into an existing
        table uses that table's columns. PostgreSQL takes every source decimal
        faithfully; a tz-aware `Datetime` loses its offset (stored naive). See
        `TYPE_MAPPING.md` in `tabsdata-conn-common-sql` for the full matrix.
    """

    tables: PostgresTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC public primitives ------------------------------------------
#
# These types are postgres-local rather than re-exported from
# `conn.common.sql.cdc.typing` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (`_cdc_plugin.py`) converts them onto the runner-side common
# representation when invoking the dialect-agnostic `run_cdc` loop.

# Relative position keywords: `"head"` rewinds to the oldest
# available change available on the replication slot; `"tail"`
# skips to the current WAL position so only events that occur
# *after* the trigger starts are emitted.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]
"""Relative CDC start position: `"head"` rewinds to the oldest change
still retained on the replication slot; `"tail"` skips to the current
WAL position (only changes made after the stream starts are emitted)."""

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]
"""Layout of the change-values payload on each emitted row: one of
`"columns"`, `"struct"` or `"map"`."""

# Slot create / reuse strategy. `"reuse"` (default) attaches to an
# existing logical replication slot; `"create"` creates one and
# falls back to attach if it already exists.
ReplicationSlotBehaviorSpec: TypeAlias = Literal["create", "reuse"]
"""Logical replication slot strategy: `"reuse"` (default) requires the
named slot to already exist; `"create"` creates it, doing nothing if it
already exists."""

# wal2json output-plugin format version. v2 is the default
# (per-action `B` / `I` / `U` / `D` / `C` messages with
# explicit transaction begin/commit framing); v1 is the older
# whole-transaction batch payload.
Wal2JsonFormatVersionSpec: TypeAlias = Literal[1, 2]
"""wal2json output-plugin format version: `2` (default, per-action
messages with explicit begin/commit framing) or `1` (the older
whole-transaction batch payload)."""


@_api.dataclass(PostgresErrorCode.POSTGRES_12, frozen=True, kw_only=True)
class LsnPosition:
    """CDC start position: a WAL Log Sequence Number.

    :categories: source

    A `start_from` value; resuming from a persisted offset uses this
    shape too (the runner persists `{"lsn": int}` on the trx state).

    Args:
        lsn: 64-bit integer LSN -- the value
            `pg_current_wal_lsn() - '0/0'::pg_lsn` returns. Use `0`
            to replay from the oldest WAL the slot retains.
    """

    lsn: int


PostgresCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    LsnPosition,
]
"""Where a Postgres CDC stream begins: a `"head"` / `"tail"` keyword
or an `LsnPosition`."""


# --- PostgresCdcStreamSrc.src_cfg keys ------------------------------
CdcSrcCfgKey: TypeAlias = Literal[
    "postgres.cdc.blocking_timeout_seconds",
    "postgres.cdc.logging_level",
    "postgres.cdc.wal2json_format_version",
    "postgres.cdc.unchanged_large_text_as_null",
    "postgres.cdc.accept_gap",
    "tabsdata.cdc.buffer.max_rows",
    "tabsdata.cdc.buffer.max_bytes",
    "tabsdata.cdc.buffer.max_secs",
    "tabsdata.cdc.trigger.max_rows",
    "tabsdata.cdc.trigger.max_bytes",
    "tabsdata.cdc.trigger.max_secs",
    "tabsdata.cdc.poll_interval_seconds",
    "tabsdata.cdc.max_large_text_length",
    "tabsdata.cdc.fetch_size",
    "tabsdata.cdc.chunk_size",
    "tabsdata.cdc.initial_load.max_window_changes",
]

CdcSrcCfgSpec: TypeAlias = Annotated[
    Mapping[CdcSrcCfgKey, Any],
    _common_validation.CONN_CFG_VALIDATION,
    _floor.seconds_floor_validator("tabsdata.cdc.trigger.max_secs"),
]


@_api.dataclass(PostgresErrorCode.POSTGRES_10, kw_only=True)
class PostgresCdcStreamSrcConn(Conn):
    """Postgres CDC connection used by `PostgresCdcStreamSrc`.

    :categories: source

    Mirrors `PostgresSrcConn` for the SQLAlchemy / catalog
    introspection side. The replication slot and publication live on
    `PostgresCdcStreamSrc` instead, so one connection can front
    multiple sources if each uses a distinct slot. Not a subclass of
    `PostgresSrcConn` -- CDC and batch SQL have different operational
    guarantees (the connecting role needs the `REPLICATION` attribute;
    the slot retention model is separate from query permissions).

    Args:
        uri: SQLAlchemy-style Postgres URI; accepts a plain string or a
            `Secret` (see `UriSpec`).
        credentials: `UserPassword` credentials. The connecting role
            must have the `REPLICATION` attribute.
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


@_api.dataclass(PostgresErrorCode.POSTGRES_11, kw_only=True)
class PostgresCdcStreamSrc(StreamSrc):
    """Postgres CDC stream source for `@stream_publisher`.

    :categories: source

    A streaming change-data-capture source: it emits one
    tableframe-batch per WAL flush across the tracked tables. Postgres
    logical replication requires a replication slot -- a persistent
    cursor on the WAL the database will not garbage-collect until the
    consumer confirms. `replication_slot` names it and
    `replication_slot_behavior` controls create vs reuse. The
    decorated `@stream_publisher` function receives the flushed batch
    as a list of frames and must NOT declare a `ctx` parameter.

    Args:
        tables: Qualified `schema.table` names to track.
        replication_slot: Logical replication slot to attach to.
        start_from: Where to begin -- `"head"` (replay from the oldest
            retained WAL), `"tail"` (current WAL LSN) or an
            `LsnPosition`. Required unless `initial_load` is `True`, and
            not allowed with it.
        initial_load: When `True`, the stream first emits a full
            snapshot of every tracked table (rows marked as inserts,
            preceded by a `LOAD` control row per table) and then captures
            changes from the exact position of that snapshot, so no
            `start_from` may be given. Defaults to `False`.

            Beyond the privileges plain CDC needs, the connecting role
            must be able to `SELECT` every tracked table, since the
            snapshot reads the tables themselves rather than the
            write-ahead log. No table is locked.

            How the snapshot is taken follows
            `replication_slot_behavior`. With `"create"` the load creates
            the slot, which exports the snapshot and the position it is
            consistent at in one operation, so no change is repeated; the
            named slot must not already exist. With `"reuse"` the load
            touches no slot and exports a snapshot from a transaction of
            its own, so the slot stays entirely with whoever provisioned
            it -- it must already exist, and a transaction committing as
            the snapshot is taken may be delivered again once capture
            starts.
        replication_slot_behavior: `"reuse"` (default, slot must
            exist) or `"create"` (create it, no-op if it exists). It
            also selects how `initial_load` snapshots -- see there.
        publication_name: Optional publication scoping the slot's output
            (wal2json's `publication_names` option).
        columns_format: Layout for the new (and old) columns payload on
            each change row -- `"columns"` (default), `"struct"` or
            `"map"`.
        flatten_columns: When `True` (default), the new columns are
            inlined as top-level columns.
        src_cfg: Optional config mapping over the allowed CDC keys only
            -- buffer / trigger thresholds, polling cadence, wal2json
            format version, the large text length limit and
            `postgres.cdc.unchanged_large_text_as_null`. The latter
            accepts that, on a table without `REPLICA IDENTITY FULL`, an
            UPDATE not touching a large text column delivers it as null;
            when unset, such a table refuses to start the stream.
            `postgres.cdc.accept_gap` lets a stream resume past the one
            gap a POSTGRES_25 failure names, given as that failure prints
            it; it matches no later gap.
    """

    # devguide-ref: CDC cursor / resume (start_from, offset persistence)
    tables: list[PostgresTableNameSpec]
    replication_slot: str
    start_from: PostgresCdcStartFromSpec | None = None
    initial_load: bool = False
    replication_slot_behavior: ReplicationSlotBehaviorSpec = "reuse"
    publication_name: str | None = None
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None

    @model_validator(mode="after")
    def _validate_start(self):
        validate_start(self.initial_load, self.start_from)
        return self
