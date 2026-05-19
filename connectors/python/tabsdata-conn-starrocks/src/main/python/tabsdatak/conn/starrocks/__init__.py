from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Conn,
    Dest,
    Env,
    NamedConn,
    Secret,
)
# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import (
    CfgSpec,
    CloudLocation,
    EntityWhitelistSpec,
    UserPassword,
)

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

# Host: no regex shape; permissive ``.+`` enables the bare-string
# shortcut at the field site.
HostSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
    Annotated[Env, _api.EnvValueConstraints(pattern=r".+")],
]

# Port: plain int, or an Env / Secret whose resolved value parses as an
# unsigned integer. ``StarRocksConn._port()`` does the final cast.
_PORT_PATTERN = r"[0-9]+"
PortSpec: TypeAlias = Union[
    int,
    Annotated[Env, _api.EnvValueConstraints(pattern=_PORT_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_PORT_PATTERN)],
]


_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

DatabaseSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_IDENTIFIER_PATTERN)],
]
StarRocksTableNameSpec: TypeAlias = Annotated[
    str, _validation.STARROCKS_TABLE_FQN_VALIDATOR,
]
StarRocksTableNamesSpec: TypeAlias = list[StarRocksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- StarRocksDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal[
    "starrocks.logging_level",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_1, kw_only=True)
class StarRocksConn(Conn):
    host: HostSpec
    port: PortSpec = 9030
    credentials: CredentialsSpec
    staging: CloudLocation
    database: DatabaseSpec = "default"
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _port(self) -> int:
        if isinstance(self.port, int):
            return self.port
        return int(self.port.value())


ConnSpec: TypeAlias = Union[NamedConn, StarRocksConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_2, kw_only=True)
class StarRocksDest(Dest):
    conn: ConnSpec
    tables: StarRocksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None
