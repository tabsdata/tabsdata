"""MSSQL connector — public API.

* :class:`MSSQLConn` -- per-instance MSSQL connection.
* :class:`MSSQLSrc` -- source: list of SELECT queries; produces
  one parquet slot per query.
* :class:`MSSQLDest` -- destination: list of target table names;
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
from tabsdatak.conn.mssql import _validation
from tabsdatak.conn.mssql.error import MssqlErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.MSSQL_URI_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.MSSQL_URI_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.MSSQL_URI_PATTERN)],
]
QueriesSpec: TypeAlias = list[str]
MSSQLTableNameSpec: TypeAlias = Annotated[
    str, _validation.MSSQL_TABLE_FQN_VALIDATOR,
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
class MSSQLConn(Conn):
    """MSSQL connection.

    :param uri: SQLAlchemy-style URI
        (``mssql+pyodbc://host:port/db?driver=ODBC+Driver``). Accepts a plain str,
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


ConnSpec: TypeAlias = Union[NamedConn, MSSQLConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_2, kw_only=True)
class MSSQLSrc(Src):
    """MSSQL source — one parquet slot per SELECT query.

    :param conn: :class:`MSSQLConn` (or :class:`NamedConn`).
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
            raise MssqlErrorCode.MSSQL_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_4, kw_only=True)
class MSSQLDest(Dest):
    """MSSQL destination — write one parquet slot per target table.

    :param conn: :class:`MSSQLConn` (or :class:`NamedConn`).
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
    tables: MSSQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
