"""Oracle connector — public API.

* :class:`OracleSrcConn` / :class:`OracleDestConn` -- per-instance Oracle connection.
* :class:`OracleSrc` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`OracleDest` -- destination: list of target table names;
  consumes one parquet slot per table.

URI and credential fields accept ``Var`` / ``Secret`` references.
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
    ContextValueSpec,
    Dest,
    Secret,
    Src,
    Var,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Var, _api.VarValueConstraints(pattern=_validation.URI_PATTERN)],
]
QueriesSpec: TypeAlias = list[str]
OracleTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.ORACLE_TABLE_FQN_VALIDATOR,
]
OracleTableNamesSpec: TypeAlias = list[OracleTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- OracleSrc.src_cfg keys -----------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "oracle.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- OracleDest.dest_cfg keys ---------------------------------------
DestCfgKey: TypeAlias = Literal[
    "oracle.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_1, kw_only=True)
class OracleSrcConn(Conn):
    """Oracle connection used by :class:`OracleSrc`."""

    __kind__ = "oracleSrcConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


@_api.dataclass(OracleErrorCode.ORACLE_1, kw_only=True)
class OracleDestConn(Conn):
    """Oracle connection used by :class:`OracleDest`.

    Attribute-identical to :class:`OracleSrcConn` but a distinct
    class so source and destination connections cannot be
    confused at the wire-format level.
    """

    __kind__ = "oracleDestConn"

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


# --- Source ----------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_2, kw_only=True)
class OracleSrc(Src):
    """Oracle source — one parquet slot per SELECT query.

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
        ``ctx.store_attr(bind_name, new_value)``.
    :param src_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, readEngine, chunkSize).
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
            raise OracleErrorCode.ORACLE_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_4, kw_only=True)
class OracleDest(Dest):
    """Oracle destination — write one parquet slot per target table.

    :param tables: Target table names (FQN or bare); one parquet
        slot per table.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (truncate before insert).
    :param transactional: When ``True`` (default), wrap every table
        write in one transactional session.
    :param dest_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, writeEngine, chunkSize).
    """

    tables: OracleTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
