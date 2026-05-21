"""Postgres connector — public API.

* :class:`PostgresConn` -- per-instance Postgres connection.
* :class:`PostgresSrc` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`PostgresDest` -- destination: list of target table names;
  consumes one parquet slot per table.

URI and credential fields accept ``Env`` / ``Secret`` references.
Cross-cutting cfg keys live in ``src_cfg`` / ``dest_cfg`` (chunk
size, read/write engine selection).
"""

from __future__ import annotations

from collections.abc import Mapping
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
from tabsdatak.conn.postgres import _validation
from tabsdatak.conn.postgres.error import PostgresErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
]
QueriesSpec: TypeAlias = list[str]
PostgresTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.POSTGRES_TABLE_FQN_VALIDATOR,
]
PostgresTableNamesSpec: TypeAlias = list[PostgresTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- PostgresSrc.src_cfg keys ---------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "postgres.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- PostgresDest.dest_cfg keys -------------------------------------
DestCfgKey: TypeAlias = Literal[
    "postgres.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_1, kw_only=True)
class PostgresConn(Conn):
    """Postgres connection.

    :param uri: SQLAlchemy-style URI
        (``postgres+psycopg2://host:port/db``). Accepts a plain str,
        ``Env``, or ``Secret``.
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


ConnSpec: TypeAlias = Union[NamedConn, PostgresConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_2, kw_only=True)
class PostgresSrc(Src):
    """Postgres source — one parquet slot per SELECT query.

    :param conn: :class:`PostgresConn` (or :class:`NamedConn`).
    :param queries: List of SELECT statements; one slot per query.
    :param transactional: When ``True`` (default), run all queries
        inside one transactional session.
    :param schema_overrides: Optional per-query column dtype hints
        (same length as ``queries``).
    :param query_params: Optional ``{name: value}`` bind parameters.
    :param src_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, readEngine, chunkSize).
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
            raise PostgresErrorCode.POSTGRES_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_4, kw_only=True)
class PostgresDest(Dest):
    """Postgres destination — write one parquet slot per target table.

    :param conn: :class:`PostgresConn` (or :class:`NamedConn`).
    :param tables: Target table names (FQN or bare); one parquet
        slot per table.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (truncate before insert).
    :param transactional: When ``True`` (default), wrap every table
        write in one transactional session.
    :param dest_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, writeEngine, chunkSize).
    """

    conn: ConnSpec
    tables: PostgresTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None


# --- CDC public primitives ------------------------------------------
#
# These types are postgres-local rather than re-exported from
# ``conn.common.sql.cdc.typing`` so the user-visible schema of one
# connector cannot widen another's by accident. The plugin
# (``_cdc_plugin.py``) converts them onto the runner-side common
# representation when invoking the dialect-agnostic ``run_cdc`` loop.

# Relative position keywords: ``"head"`` rewinds to the oldest
# available change available on the replication slot; ``"tail"``
# skips to the current WAL position so only events that occur
# *after* the trigger starts are emitted.
RelativePositionSpec: TypeAlias = Literal["head", "tail"]

# Layout for the columns payload on each emitted change row.
CdcColumnsFormatSpec: TypeAlias = Literal["columns", "struct", "map"]

# Slot create / reuse strategy. ``"reuse"`` (default) attaches to an
# existing logical replication slot; ``"create"`` creates one and
# falls back to attach if it already exists.
ReplicationSlotBehaviorSpec: TypeAlias = Literal["create", "reuse"]

# wal2json output-plugin format version. v2 is the default
# (per-action ``B`` / ``I`` / ``U`` / ``D`` / ``C`` messages with
# explicit transaction begin/commit framing); v1 is the older
# whole-transaction batch payload.
Wal2JsonFormatVersionSpec: TypeAlias = Literal[1, 2]


@_api.dataclass(PostgresErrorCode.POSTGRES_12, frozen=True, kw_only=True)
class LsnPosition:
    lsn: int


PostgresCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    LsnPosition,
]


# --- PostgresCdcStreamSrc.src_cfg keys ------------------------------
CdcSrcCfgKey: TypeAlias = Literal[
    "postgres.cdc.blocking_timeout_seconds",
    "postgres.cdc.wal2json_format_version",
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


@_api.dataclass(PostgresErrorCode.POSTGRES_10, kw_only=True)
class PostgresCdcConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


CdcConnSpec: TypeAlias = Union[NamedConn, PostgresCdcConn]


@_api.dataclass(PostgresErrorCode.POSTGRES_11, kw_only=True)
class PostgresCdcStreamSrc(StreamSrc):
    conn: CdcConnSpec
    tables: list[PostgresTableNameSpec]
    replication_slot: str
    start_from: PostgresCdcStartFromSpec
    replication_slot_behavior: ReplicationSlotBehaviorSpec = "reuse"
    publication_name: str | None = None
    columns_format: CdcColumnsFormatSpec = "columns"
    flatten_columns: bool = True
    src_cfg: CdcSrcCfgSpec | None = None
