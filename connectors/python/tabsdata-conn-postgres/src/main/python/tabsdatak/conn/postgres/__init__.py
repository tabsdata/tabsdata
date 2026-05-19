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
    str, _validation.POSTGRES_TABLE_FQN_VALIDATOR,
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
    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, PostgresConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(PostgresErrorCode.POSTGRES_2, kw_only=True)
class PostgresSrc(Src):
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
    conn: ConnSpec
    tables: PostgresTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
