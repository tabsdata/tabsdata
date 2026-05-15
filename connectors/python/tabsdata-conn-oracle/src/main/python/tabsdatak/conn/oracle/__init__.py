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
from tabsdatak.conn.oracle import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
]
IdentifierSpec: TypeAlias = Annotated[str, _validation.IDENTIFIER_VALIDATOR]
QueriesSpec: TypeAlias = list[str]
OracleTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_FQN_VALIDATOR]
OracleTableNamesSpec: TypeAlias = list[OracleTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- Connection ------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_1, kw_only=True)
class OracleConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, OracleConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_2, kw_only=True)
class OracleSrc(Src):
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
            raise OracleErrorCode.ORACLE_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_4, kw_only=True)
class OracleDest(Dest):
    conn: ConnSpec
    tables: OracleTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: CfgSpec | None = None
