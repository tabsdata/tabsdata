from __future__ import annotations

from typing import Annotated, Literal, TypeAlias, Union

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
from tabsdatak.conn.common.types import CfgSpec, CloudLocation, UserPassword

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
PortSpec: TypeAlias = int
IdentifierSpec: TypeAlias = Annotated[str, _validation.IDENTIFIER_VALIDATOR]


_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

DatabaseSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_IDENTIFIER_PATTERN)],
]
StarRocksTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_FQN_VALIDATOR]
StarRocksTableNamesSpec: TypeAlias = list[StarRocksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- Connection ------------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_1, kw_only=True)
class StarRocksConn(Conn):
    host: HostSpec
    staging: CloudLocation
    port: PortSpec = 9030
    database: DatabaseSpec = "default"
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, StarRocksConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(StarRocksErrorCode.STARROCKS_2, kw_only=True)
class StarRocksDest(Dest):
    conn: ConnSpec
    tables: StarRocksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None
