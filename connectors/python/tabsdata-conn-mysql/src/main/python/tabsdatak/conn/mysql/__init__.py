"""MySQL connector — public API.

Batch:

* :class:`MySQLSrcConn` -- per-instance MySQL connection for
  :class:`MySQLSrc`.
* :class:`MySQLDestConn` -- per-instance MySQL connection for
  :class:`MySQLDest`. Attribute-identical to
  :class:`MySQLSrcConn` but a distinct class so source and
  destination connections cannot be confused at the wire level
  (the ``__kind__`` discriminator differs).
* :class:`MySQLSrc` -- source: list of SOQL/SELECT queries; produces
  one parquet slot per query.
* :class:`MySQLDest` -- destination: list of target table names;
  consumes one parquet slot per table.

CDC (change-data capture):

* :class:`MySQLCdcStreamSrcConn` -- per-instance MySQL CDC
  connection. Adds ``server_id`` for the binlog replication
  identity.
* :class:`MySQLCdcStreamSrc` -- stream source that emits one
  tableframe-batch per binlog flush across the tracked tables.
* :class:`GtidPosition` / :class:`BinlogPosition` /
  :class:`TimestampPosition` -- ``start_from`` primitives for
  resuming binlog replication at a specific position.

URI and credential fields accept ``Var`` / ``Secret`` references.
Cross-cutting cfg keys live in ``src_cfg`` / ``dest_cfg`` (chunk
size, read/write engine selection); CDC tuning lives in
``MySQLCdcStreamSrc.src_cfg`` (buffer / trigger thresholds, polling
cadence).
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
    Conn,
    ContextValueSpec,
    Dest,
    Secret,
    Src,
    StreamSrc,
    Var,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _validation
from tabsdatak.conn.mysql.error import MysqlErrorCode

# --- CDC public primitives ------------------------------------------
#
# These types are mysql-local rather than re-exported from
# ``conn.common.sql.cdc.typing`` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (``_cdc_plugin.py``) converts them onto the runner-side common
# representation when invoking the dialect-agnostic ``run_cdc`` loop.

# Default MySQL replica ``server_id`` for the binlog stream. Each
# concurrent CDC replica must use a unique id across the cluster;
# 512 is the legacy default and works fine for single-replica
# deployments. ``MySQLCdcConn.server_id`` overrides per-connection.
DEFAULT_SERVER_ID = 512

# Relative position keywords: ``"head"`` rewinds to the oldest
# available change in retention; ``"tail"`` skips to the live edge.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]


@_api.dataclass(MysqlErrorCode.MYSQL_12, frozen=True, kw_only=True)
class GtidPosition:
    """Start binlog replication at a specific MySQL Global
    Transaction ID. The ingester converts ``"uuid:N"`` to the
    pymysqlreplication-friendly set range ``"uuid:1-N"``.

    :param gtid: GTID to resume from, in ``"uuid:N"`` shape (single
        position) or ``"uuid:M-N"`` shape (range). Empty string is
        the cold-start marker.
    """

    gtid: str


@_api.dataclass(MysqlErrorCode.MYSQL_13, frozen=True, kw_only=True)
class BinlogPosition:
    """Start binlog replication at a binlog file + byte offset
    (the pre-GTID position shape).

    :param file: Binlog filename as reported by
        ``SHOW BINARY LOGS`` (e.g. ``"mysql-bin.000001"``).
    :param pos: Byte offset within ``file``. The binlog file's
        magic bytes occupy positions 0-3; use 4 to replay from the
        beginning.
    """

    file: str
    pos: int


@_api.dataclass(MysqlErrorCode.MYSQL_18, frozen=True, kw_only=True)
class TimestampPosition:
    """Start binlog replication at the first event with a timestamp
    at or after ``ts``.

    :param ts: Wall-clock cut-off; tz-aware datetimes are honoured,
        naive datetimes are interpreted in the MySQL server's
        timezone.
    """

    ts: datetime


MySQLCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    GtidPosition,
    BinlogPosition,
    TimestampPosition,
]

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.MSYQL_URI_PATTERN)],
    Annotated[Var, _api.VarValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)
    ],
]
QueriesSpec: TypeAlias = list[str]
MySQLTableNameSpec: TypeAlias = Annotated[str, _validation.MYSQL_TABLE_FQN_VALIDATOR]
MySQLTableNamesSpec: TypeAlias = list[MySQLTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- MySQLSrc.src_cfg keys ------------------------------------------
#
# Restricts ``MySQLSrc.src_cfg`` to the keys the source plugin
# (``_plugin.py`` / engine factory) actually consumes. Pydantic
# enforces the literal membership of keys at validation time, so
# unknown keys are rejected at construction; ``CONN_CFG_VALIDATOR``
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
    """MySQL connection used by :class:`MySQLSrc`.

    :param uri: SQLAlchemy-style URI
        (``mysql+mysqlconnector://host:port/db``). Accepts a plain
        str, ``Var``, or ``Secret``.
    :param credentials: :class:`UserPassword` -- user + password
        spliced into the URI by the engine factory.
    :param entity_whitelist: Optional list of regex patterns; the
        runtime uses these to narrow which tables this conn
        contributes to discovery.
    :param conn_cfg: Optional driver-side config (forwarded as
        ``connect_args`` to SQLAlchemy).
    """

    __kind__ = "mysqlSrcConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


@_api.dataclass(MysqlErrorCode.MYSQL_1, kw_only=True)
class MySQLDestConn(Conn):
    """MySQL connection used by :class:`MySQLDest`.

    Attribute-identical to :class:`MySQLSrcConn` but a distinct
    class so source and destination connections cannot be confused
    at the wire-format level (the ``__kind__`` differs).
    """

    __kind__ = "mysqlDestConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


# --- Source ----------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_2, kw_only=True)
class MySQLSrc(Src):
    """MySQL source — one parquet slot per SELECT query.

    :param queries: List of SELECT statements; one slot per query.
    :param transactional: When ``True`` (default), run all queries
        inside one transactional session; ``False`` opens a fresh
        autocommit connection per query.
    :param schema_overrides: Optional per-query column dtype hints
        (one entry per query, same length as ``queries``); else MYSQL_3.
    :param initial_values: Optional ``{bind_name: value}`` seed dict.
        Each name is bound into the query as a SQLAlchemy ``:name``
        parameter at execute time. Acts as the **first-run seed** for
        each bind name: at runtime, the plugin looks up
        ``ctx.get_attr(bind_name)`` per bind, and falls back to this
        field when the ctx scratch store has no entry for that name.
        User functions persist new values for the next run with
        ``ctx.store_attr(bind_name, new_value)``.
    :param src_cfg: Optional ``Mapping`` over the literal cfg key
        set (``mysql.logging_level``, ``tabsdata.sql.sql.readEngine``,
        ``tabsdata.sql.chunkSize``).
    """

    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    initial_values: dict[str, ContextValueSpec] | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
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
    """MySQL destination — write one parquet slot per target table.

    :param tables: Target table names (FQN or bare); one parquet
        slot per table.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (truncate before insert).
    :param transactional: When ``True`` (default), wrap every table
        write in one transactional session.
    :param dest_cfg: Optional ``Mapping`` over the literal cfg key
        set (``mysql.logging_level``,
        ``tabsdata.sql.sql.writeEngine``, ``tabsdata.sql.chunkSize``).
    """

    tables: MySQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC -------------------------------------------------------------


# Allowed keys on ``MySQLCdcStreamSrc.src_cfg``. The plugin reads
# these (falling back to defaults) before constructing the runner's
# ``CdcRunConfig``.
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
    """MySQL CDC connection used by :class:`MySQLCdcStreamSrc`.

    Mirrors :class:`MySQLSrcConn` for the SQLAlchemy / catalog
    introspection side and adds ``server_id`` -- the unique replica
    identifier the source uses for binlog replication. Each
    concurrent CDC connection against the same MySQL must declare a
    distinct ``server_id``; collisions cause the source to disconnect
    one of the replicas.

    Not a subclass of :class:`MySQLSrcConn`: CDC and batch SQL have
    different operational guarantees (replication grants vs query
    permissions, replica id reservation) and a divergent attribute
    set is expected over time.

    :param uri: SQLAlchemy-style URI; accepts ``str`` / ``Var`` /
        ``Secret`` per :data:`UriSpec`.
    :param credentials: :class:`UserPassword`.
    :param server_id: MySQL replica id for the binlog stream.
        Default 512 matches the legacy connector.
    :param entity_whitelist: Optional regex patterns narrowing
        discovery.
    :param conn_cfg: Optional driver-side config forwarded as
        ``connect_args``.
    """

    __kind__ = "mysqlCdcStreamSrcConn"

    uri: UriSpec
    credentials: CredentialsSpec
    server_id: int = DEFAULT_SERVER_ID
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


@_api.dataclass(MysqlErrorCode.MYSQL_11, kw_only=True)
class MySQLCdcStreamSrc(StreamSrc):
    """MySQL CDC stream source -- emits one tableframe-batch per
    binlog flush across the tracked tables.

    :param tables: Qualified ``schema.table`` names to track.
    :param start_from: Where to begin reading the binlog when no
        prior offset has been persisted on the trx context --
        ``"head"`` / ``"tail"`` keywords, a :class:`GtidPosition`, a
        :class:`BinlogPosition`, or a :class:`TimestampPosition`.
    :param columns_format: Layout for the new (and old, when present)
        columns payload on each emitted change row. See
        :data:`CdcColumnsFormatSpec`.
    :param flatten_columns: When ``True`` the new columns are inlined
        as top-level columns -- the connector's downstream consumer
        sees an ordinary tabular row and ``columns_format`` only
        affects the *old*-columns layout.
    :param src_cfg: Optional ``Mapping`` over the literal
        :data:`CdcSrcCfgKey` set -- buffer / trigger thresholds and
        polling cadence. The plugin falls back to its built-in
        defaults for missing keys.
    """

    tables: list[MySQLTableNameSpec]
    start_from: MySQLCdcStartFromSpec
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
