from __future__ import annotations

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
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword

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
IdentifierSpec: TypeAlias = Annotated[str, _validation.IDENTIFIER_VALIDATOR]
QueriesSpec: TypeAlias = list[str]
MSSQLTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_FQN_VALIDATOR]
MSSQLTableNamesSpec: TypeAlias = list[MSSQLTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- Connection ------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_1, kw_only=True)
class MSSQLConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MSSQLConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_2, kw_only=True)
class MSSQLSrc(Src):
    conn: ConnSpec
    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    query_params: dict[str, Any] | None = None
    src_cfg: CfgSpec | None = None

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
    conn: ConnSpec
    tables: MSSQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: CfgSpec | None = None
