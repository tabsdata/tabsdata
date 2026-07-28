#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL connector -- public API.

Imported from `tabsdatak.conn.mysql`. Batch classes:

* `MySQLSrcConn` / `MySQLDestConn` -- the connection: a SQLAlchemy
  URI plus `UserPassword` credentials. Attribute-identical but distinct
  classes, one for source and one for destination connections.
* `MySQLSrc` -- the `@publisher` source: a list of SELECT
  `queries`, one output slot per query.
* `MySQLDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table.

CDC (change-data capture) classes:

* `MySQLCdcStreamSrcConn` -- the CDC connection; adds `server_id`,
  the binlog replication identity.
* `MySQLCdcStreamSrc` -- the `@stream_publisher` source; emits one
  tableframe-batch per binlog flush across the tracked tables.
* `GtidPosition` / `BinlogPosition` / `TimestampPosition` --
  `start_from` primitives for resuming binlog replication at a
  specific position.

The connection (with credentials) is registered on a collection and
resolved from the function's collection at run time -- it is never passed
to a decorator. A flow function only references the source / destination.

Examples:
    .. code-block:: python

        @publisher(source=MySQLSrc(queries=["SELECT * FROM city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=MySQLDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(cities: TableFrameSpec) -> TableFrameSpec:
            return cities
A `@stream_publisher` receives the flushed batch as a list of frames
and must NOT declare a `ctx` parameter.

Examples:
    .. code-block:: python

        @stream_publisher(source=MySQLCdcStreamSrc(tables=["app.city"],
                                                   start_from="tail"),
                          flush_interval_mins=2, output_tables=["cdc_raw"])
        def stream(rows: TableFramesSpec) -> TableFrameSpec:
            return concat(rows)
URI and credential fields accept `Secret` references. Cross-cutting
cfg keys live in `src_cfg` / `dest_cfg` (chunk size, read/write
engine selection); CDC tuning lives in `MySQLCdcStreamSrc.src_cfg`
(buffer / trigger thresholds, polling cadence).
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
from tabsdatak.conn.mysql import _validation
from tabsdatak.conn.mysql.error import MysqlErrorCode
from tabsdatak.spi import Conn

# --- CDC public primitives ------------------------------------------
#
# These types are mysql-local rather than re-exported from
# `conn.common.sql.cdc.typing` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (`_cdc_plugin.py`) converts them onto the runner-side common
# representation when invoking the dialect-agnostic `run_cdc` loop.

# Default MySQL replica `server_id` for the binlog stream. Each
# concurrent CDC replica must use a unique id across the cluster;
# 512 is the legacy default and works fine for single-replica
# deployments. `MySQLCdcConn.server_id` overrides per-connection.
DEFAULT_SERVER_ID = 512
"""Default MySQL replica `server_id` for the binlog stream (512). Each
concurrent CDC replica must use a unique id across the cluster."""

# Relative position keywords: `"head"` rewinds to the oldest
# available change in retention; `"tail"` skips to the live edge.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]
"""Relative CDC start position: `"head"` rewinds to the oldest change
still in binlog retention; `"tail"` skips to the live edge (only later
changes are emitted)."""

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]
"""Layout of the change-values payload on each emitted row: one of
`"columns"`, `"struct"` or `"map"`."""


@_api.dataclass(MysqlErrorCode.MYSQL_12, frozen=True, kw_only=True)
class GtidPosition:
    """CDC start position: a MySQL Global Transaction ID.

    :categories: source

    A `start_from` value; the ingester converts `"uuid:N"` to the
    pymysqlreplication-friendly set range `"uuid:1-N"`.

    Args:
        gtid: GTID to resume from, in `"uuid:N"` shape (single
            position) or `"uuid:M-N"` shape (range). Empty string is
            the cold-start marker.
    """

    gtid: str


@_api.dataclass(MysqlErrorCode.MYSQL_13, frozen=True, kw_only=True)
class BinlogPosition:
    """CDC start position: a binlog file + byte offset.

    :categories: source

    A `start_from` value in the pre-GTID position shape.

    Args:
        file: Binlog filename as reported by `SHOW BINARY LOGS` (e.g.
            `"mysql-bin.000001"`).
        pos: Byte offset within `file`. The binlog magic bytes occupy
            positions 0-3; use 4 to replay from the beginning.
    """

    file: str
    pos: int


@_api.dataclass(MysqlErrorCode.MYSQL_18, frozen=True, kw_only=True)
class TimestampPosition:
    """CDC start position given as a wall-clock timestamp.

    :categories: source

    A `start_from` value resolved to the first binlog event at or after
    `ts`.

    Args:
        ts: Wall-clock cut-off. Tz-aware datetimes are honoured; naive
            datetimes are interpreted in the MySQL server's timezone.
    """

    ts: datetime


MySQLCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    GtidPosition,
    BinlogPosition,
    TimestampPosition,
]
"""Where a MySQL CDC stream begins: a `"head"` / `"tail"` keyword, a
`GtidPosition`, a `BinlogPosition` or a `TimestampPosition`."""

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for MySQL connections: `UserPassword` (user +
password), spliced into the URI by the engine factory."""

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.MYSQL_URI_PATTERN)
]
"""A SQLAlchemy-style MySQL URI, e.g.
`mysql+mysqlconnector://host:port/db`. The scheme must be `mysql` or
`mariadb` (an optional `+driver` is allowed). Accepts a plain string
or a `Secret`."""

QueriesSpec: TypeAlias = list[str]
"""A non-empty, ordered list of SELECT statements -- one output slot per
query."""

MySQLTableNameSpec: TypeAlias = Annotated[str, _validation.MYSQL_TABLE_FQN_VALIDATOR]
"""A MySQL table name, either `table` or `database.table` (at most
two dot-separated parts). Parts may be backtick-quoted, using the
doubled-backtick convention to embed a literal backtick."""

MySQLTableNamesSpec: TypeAlias = list[MySQLTableNameSpec]
"""A list of MySQL table names (see `MySQLTableNameSpec`)."""

IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
"""Write disposition when the target table exists: `"append"` adds
rows; `"replace"` truncates before insert."""


# --- MySQLSrc.src_cfg keys ------------------------------------------
#
# Restricts `MySQLSrc.src_cfg` to the keys the source plugin
# (`_plugin.py` / engine factory) actually consumes. Pydantic
# enforces the literal membership of keys at validation time, so
# unknown keys are rejected at construction; `CONN_CFG_VALIDATOR`
# reuses the conn_cfg basic-value checks (str/int/float/bool/None,
# plus list/dict compositions).
SrcCfgKey: TypeAlias = Literal[
    "mysql.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- MySQLDest.dest_cfg keys ----------------------------------------
DestCfgKey: TypeAlias = Literal[
    "mysql.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_1, kw_only=True)
class MySQLSrcConn(Conn):
    """MySQL connection used by `MySQLSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        uri: SQLAlchemy-style MySQL URI
            (`mysql+mysqlconnector://host:port/db`). Accepts a plain
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


@_api.dataclass(MysqlErrorCode.MYSQL_1, kw_only=True)
class MySQLDestConn(Conn):
    """MySQL connection used by `MySQLDest`.

    :categories: destination

    Attribute-identical to `MySQLSrcConn` but a distinct class; source
    and destination connections are separate types and cannot be used in
    place of one another. See `MySQLSrcConn` for the field meanings.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_2, kw_only=True)
class MySQLSrc(Src):
    """MySQL source for `@publisher` -- one output slot per query.

    :categories: source

    Args:
        queries: SELECT statements; one output slot per query, in order.
        transactional: When `True` (default), run all queries inside
            one transactional session; `False` opens a fresh
            autocommit connection per query.
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
            (`mysql.logging_level`, `tabsdata.sql.sql.readEngine`,
            `tabsdata.sql.chunkSize`).

    Examples:
        Publish multiple MySQL tables, one output slot per query in order:

        .. code-block:: python

            @publisher(
                source=MySQLSrc(
                    queries=[
                        "SELECT * FROM country",
                        "SELECT * FROM city",
                    ],
                ),
                output_tables=["country", "city"],
            )
            def ingest(country: TableFrameSpec, city: TableFrameSpec):
                return country, city

        Incremental read: an `initial_values` bind seeds the `:wm`
        watermark on the first run, then `ctx` advances it each run:

        .. code-block:: python

            @publisher(
                source=MySQLSrc(
                    queries=["SELECT * FROM city WHERE id > :wm LIMIT 10"],
                    initial_values={"wm": 0},
                ),
                output_tables=["city_inc"],
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

        Called by the framework; raises `MySQLValidateException` when
        the lengths differ.
        """
        if self.schema_overrides is not None and len(self.schema_overrides) != len(
            self.queries
        ):
            raise MysqlErrorCode.MYSQL_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_4, kw_only=True)
class MySQLDest(Dest):
    """MySQL destination for `@subscriber` -- one target table per slot.

    :categories: destination

    Args:
        tables: Target table names, qualified or bare (see
            `MySQLTableNameSpec`); one input slot per table, in order.
        if_table_exists: `"append"` (default) adds rows; `"replace"`
            truncates the table before inserting.
        transactional: When `True` (default), wrap every table write
            in one transactional session.
        dest_cfg: Optional config mapping over the allowed keys only
            (`mysql.logging_level`, `tabsdata.sql.sql.writeEngine`,
            `tabsdata.sql.chunkSize`).

    Note:
        Column types are picked only when the target table is created (with
        `"replace"`, or `"append"` to a missing table); writing into an existing
        table uses that table's columns. MySQL caps `DECIMAL` scale at 30, so a
        finer source scale is truncated. A tz-aware `Datetime` loses its offset
        (stored naive). See `TYPE_MAPPING.md` in `tabsdata-conn-common-sql` for
        the full matrix.
    """

    tables: MySQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC -------------------------------------------------------------


# Allowed keys on `MySQLCdcStreamSrc.src_cfg`. The plugin reads
# these (falling back to defaults) before constructing the runner's
# `CdcRunConfig`.
CdcSrcCfgKey: TypeAlias = Literal[
    "mysql.cdc.blocking_timeout_seconds",
    "mysql.cdc.logging_level",
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


@_api.dataclass(MysqlErrorCode.MYSQL_10, kw_only=True)
class MySQLCdcStreamSrcConn(Conn):
    """MySQL CDC connection used by `MySQLCdcStreamSrc`.

    :categories: source

    Mirrors `MySQLSrcConn` for the SQLAlchemy / catalog introspection
    side and adds `server_id` -- the unique replica identifier the
    source uses for binlog replication. Each concurrent CDC connection
    against the same MySQL must declare a distinct `server_id`;
    collisions cause the source to disconnect one of the replicas. Not a
    subclass of `MySQLSrcConn` -- CDC and batch SQL have different
    operational guarantees (replication grants vs query permissions,
    replica-id reservation).

    Args:
        uri: SQLAlchemy-style MySQL URI; accepts a plain string or a
            `Secret` (see `UriSpec`).
        credentials: `UserPassword` credentials.
        server_id: MySQL replica id for the binlog stream. Defaults to
            `DEFAULT_SERVER_ID` (512).
        entity_whitelist: Optional regex patterns narrowing discovery;
            `None` (default) means no filtering.
        conn_cfg: Optional driver-side config forwarded as
            `connect_args`.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    server_id: int = DEFAULT_SERVER_ID
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(MysqlErrorCode.MYSQL_11, kw_only=True)
class MySQLCdcStreamSrc(StreamSrc):
    """MySQL CDC stream source for `@stream_publisher`.

    :categories: source

    A streaming change-data-capture source: it emits one
    tableframe-batch per binlog flush across the tracked tables. The
    decorated `@stream_publisher` function receives the flushed batch
    as a list of frames and must NOT declare a `ctx` parameter.

    Args:
        tables: Qualified `schema.table` names to track.
        start_from: Where to begin reading the binlog when no prior
            offset is persisted -- `"head"` / `"tail"` keyword, a
            `GtidPosition`, a `BinlogPosition` or a
            `TimestampPosition`.
        columns_format: Layout for the new (and old, when present)
            columns payload on each change row -- `"columns"`
            (default), `"struct"` or `"map"`.
        flatten_columns: When `True` (default), the new columns are
            inlined as top-level columns, so the downstream consumer sees
            an ordinary tabular row and `columns_format` then only
            affects the old-columns layout.
        src_cfg: Optional config mapping over the allowed CDC keys only
            -- buffer / trigger thresholds and polling cadence. The
            plugin falls back to its built-in defaults for missing keys.
    """

    # devguide-ref: CDC cursor / resume (start_from, offset persistence)
    tables: list[MySQLTableNameSpec]
    start_from: MySQLCdcStartFromSpec
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
