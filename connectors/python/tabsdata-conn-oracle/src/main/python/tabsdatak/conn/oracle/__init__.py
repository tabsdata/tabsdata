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
from tabsdatak.conn.oracle import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
]
QueriesSpec: TypeAlias = list[str]
OracleTableNameSpec: TypeAlias = Annotated[
    str, _validation.ORACLE_TABLE_FQN_VALIDATOR,
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
class OracleConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
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
    conn: ConnSpec
    tables: OracleTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
