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

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _validation
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode
from tabsdatak.conn.common.types import CfgSpec, CloudLocation, UserPassword

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword


# Host: no regex shape; any non-empty string passes. The permissive
# ``.+`` pattern is what enables the bare-string shortcut at the field
# site (the ``EnvValuePattern`` / ``SecretValuePattern`` wrapper promotes
# a plain str to a literal); without it, callers would have to spell
# ``Env.lit("ch.example")`` themselves.
HostSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
    Annotated[Env, _api.EnvValueConstraints(pattern=r".+")],
]
PortSpec: TypeAlias = int
IdentifierSpec: TypeAlias = Annotated[str, _validation.IDENTIFIER_VALIDATOR]


# Database: ClickHouse identifier shape (alphanumeric + underscore).
_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"

DatabaseSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_IDENTIFIER_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_IDENTIFIER_PATTERN)],
]
ClickHouseTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_FQN_VALIDATOR]
ClickHouseTableNamesSpec: TypeAlias = list[ClickHouseTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["strict", "iceberg"]


# --- Connection ------------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_1, kw_only=True)
class ClickHouseConn(Conn):
    host: HostSpec
    staging: CloudLocation
    port: PortSpec = 8123
    database: DatabaseSpec = "default"
    credentials: CredentialsSpec
    secure: bool = False
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, ClickHouseConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(ClickHouseErrorCode.CLICKHOUSE_2, kw_only=True)
class ClickHouseDest(Dest):
    conn: ConnSpec
    tables: ClickHouseTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None
