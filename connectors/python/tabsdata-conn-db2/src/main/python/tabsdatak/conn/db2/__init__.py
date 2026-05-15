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
from tabsdatak.conn.db2 import _validation
from tabsdatak.conn.db2.error import Db2ErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
]
IdentifierSpec: TypeAlias = Annotated[str, _validation.IDENTIFIER_VALIDATOR]
QueriesSpec: TypeAlias = list[str]
Db2TableNameSpec: TypeAlias = Annotated[str, _validation.DB2_TABLE_FQN_VALIDATOR]
Db2TableNamesSpec: TypeAlias = list[Db2TableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- Connection ------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_1, kw_only=True)
class Db2Conn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, Db2Conn]


# --- Source ----------------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_2, kw_only=True)
class Db2Src(Src):
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
            raise Db2ErrorCode.DB2_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(Db2ErrorCode.DB2_4, kw_only=True)
class Db2Dest(Dest):
    conn: ConnSpec
    tables: Db2TableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: CfgSpec | None = None
