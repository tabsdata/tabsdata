"""MySQL connector — public API.

* :class:`MySQLConn` -- per-instance MySQL connection.
* :class:`MySQLSrc` -- source: list of SOQL/SELECT queries; produces
  one parquet slot per query.
* :class:`MySQLDest` -- destination: list of target table names;
  consumes one parquet slot per table.

URI and credential fields accept ``Env`` / ``Secret`` references.
Cross-cutting cfg keys live in ``src_cfg`` / ``dest_cfg`` (chunk
size, read/write engine selection).
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
    Dest,
    Env,
    NamedConn,
    Secret,
    Src,
    StreamSrc,
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
    gtid: str


@_api.dataclass(MysqlErrorCode.MYSQL_13, frozen=True, kw_only=True)
class BinlogPosition:
    file: str
    pos: int


@_api.dataclass(MysqlErrorCode.MYSQL_18, frozen=True, kw_only=True)
class TimestampPosition:
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
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)],
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
class MySQLConn(Conn):
    """MySQL connection.

    :param uri: SQLAlchemy-style URI
        (``mysql+mysqlconnector://host:port/db``). Accepts a plain
        str, ``Env``, or ``Secret``.
    :param credentials: :class:`UserPassword` -- user + password
        spliced into the URI by the engine factory.
    :param entity_whitelist: Optional list of regex patterns; the
        runtime uses these to narrow which tables this conn
        contributes to discovery.
    :param conn_cfg: Optional driver-side config (forwarded as
        ``connect_args`` to SQLAlchemy).
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MySQLConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_2, kw_only=True)
class MySQLSrc(Src):
    """MySQL source — one parquet slot per SELECT query.

    :param conn: :class:`MySQLConn` (or :class:`NamedConn`).
    :param queries: List of SELECT statements; one slot per query.
    :param transactional: When ``True`` (default), run all queries
        inside one transactional session; ``False`` opens a fresh
        autocommit connection per query.
    :param schema_overrides: Optional per-query column dtype hints
        (one entry per query, same length as ``queries``); else MYSQL_3.
    :param query_params: Optional ``{name: value}`` bind parameters
        substituted into every query via SQLAlchemy ``:name`` syntax.
    :param src_cfg: Optional ``Mapping`` over the literal cfg key
        set (``mysql.logging_level``, ``tabsdata.sql.sql.readEngine``,
        ``tabsdata.sql.chunkSize``).
    """

    conn: ConnSpec
    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    query_params: dict[str, Any] | None = None
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

    :param conn: :class:`MySQLConn` (or :class:`NamedConn`).
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

    conn: ConnSpec
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
class MySQLCdcConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    server_id: int = DEFAULT_SERVER_ID
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


CdcConnSpec: TypeAlias = Union[NamedConn, MySQLCdcConn]


@_api.dataclass(MysqlErrorCode.MYSQL_11, kw_only=True)
class MySQLCdcStreamSrc(StreamSrc):
    conn: CdcConnSpec
    tables: list[MySQLTableNameSpec]
    start_from: MySQLCdcStartFromSpec
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
