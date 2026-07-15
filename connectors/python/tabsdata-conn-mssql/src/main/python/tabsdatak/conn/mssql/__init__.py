"""MSSQL connector — public API.

* :class:`MSSQLSrcConn` / :class:`MSSQLDestConn` -- per-instance MSSQL connection.
* :class:`MSSQLSrc` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`MSSQLDest` -- destination: list of target table names;
  consumes one parquet slot per table.

URI and credential fields accept ``Secret`` references.
Cross-cutting cfg keys live in ``src_cfg`` / ``dest_cfg`` (chunk
size, read/write engine selection).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# Private modules are imported as modules (not by symbol) so each
# call-site is explicit about reaching into private surface.
# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    Src,
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
from tabsdatak.conn.mssql import _validation
from tabsdatak.conn.mssql.error import MssqlErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.MSSQL_URI_PATTERN)
]
QueriesSpec: TypeAlias = list[str]
MSSQLTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.MSSQL_TABLE_FQN_VALIDATOR,
]
MSSQLTableNamesSpec: TypeAlias = list[MSSQLTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- MSSQLSrc.src_cfg keys ------------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "mssql.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- MSSQLDest.dest_cfg keys ----------------------------------------
DestCfgKey: TypeAlias = Literal[
    "mssql.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_1, kw_only=True)
class MSSQLSrcConn(Conn):
    """MSSQL connection used by :class:`MSSQLSrc`."""

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(MssqlErrorCode.MSSQL_1, kw_only=True)
class MSSQLDestConn(Conn):
    """MSSQL connection used by :class:`MSSQLDest`.

    Attribute-identical to :class:`MSSQLSrcConn` but a distinct
    class so source and destination connections cannot be
    confused at the wire-format level.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_2, kw_only=True)
class MSSQLSrc(Src):
    """MSSQL source — one parquet slot per SELECT query.

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
            raise MssqlErrorCode.MSSQL_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_4, kw_only=True)
class MSSQLDest(Dest):
    """MSSQL destination — write one parquet slot per target table.

    :param tables: Target table names (FQN or bare); one parquet
        slot per table.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (truncate before insert).
    :param transactional: When ``True`` (default), wrap every table
        write in one transactional session.
    :param dest_cfg: Optional ``Mapping`` over the literal cfg key
        set (logging_level, writeEngine, chunkSize).
    """

    tables: MSSQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
