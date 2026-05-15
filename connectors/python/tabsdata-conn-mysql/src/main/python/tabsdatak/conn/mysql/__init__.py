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
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _validation
from tabsdatak.conn.mysql.error import MysqlErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.MSYQL_URI_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.MSYQL_URI_PATTERN)],
]
IdentifierSpec: TypeAlias = Annotated[str, _validation.MYSQL_IDENTIFIER_VALIDATOR]
QueriesSpec: TypeAlias = list[str]
MySQLTableNameSpec: TypeAlias = Annotated[str, _validation.MYSQL_TABLE_FQN_VALIDATOR]
MySQLTableNamesSpec: TypeAlias = list[MySQLTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- Connection ------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_1, kw_only=True)
class MySQLConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MySQLConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_2, kw_only=True)
class MySQLSrc(Src):
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
            raise MysqlErrorCode.MYSQL_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(MysqlErrorCode.MYSQL_4, kw_only=True)
class MySQLDest(Dest):
    conn: ConnSpec
    tables: MySQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: CfgSpec | None = None
