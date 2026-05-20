"""Db2 connector — public API.

* :class:`Db2Conn` -- per-instance Db2 connection.
* :class:`Db2Src` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`Db2Dest` -- destination: list of target table names;
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
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
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
class Db2Conn(Conn):
    """Db2 connection.

    :param uri: SQLAlchemy-style URI
        (``db2+ibm_db://host:port/db``). Accepts a plain str,
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


ConnSpec: TypeAlias = Union[NamedConn, Db2Conn]


# --- Source ----------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_2, kw_only=True)
class Db2Src(Src):
    """Db2 source — one parquet slot per SELECT query.

    :param conn: :class:`Db2Conn` (or :class:`NamedConn`).
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
            raise Db2ErrorCode.DB2_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_4, kw_only=True)
class Db2Dest(Dest):
    """Db2 destination — write one parquet slot per target table.

    :param conn: :class:`Db2Conn` (or :class:`NamedConn`).
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
    tables: Db2TableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
