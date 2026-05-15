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
    SecretOrEnvSpec,
    Src,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _validation
from tabsdatak.conn.databricks.error import DatabricksErrorCode

# --- Type aliases ----------------------------------------------------


HostUrlSpec: TypeAlias = SecretOrEnvSpec
DatabricksTokenSpec: TypeAlias = SecretOrEnvSpec
CredentialsSpec: TypeAlias = DatabricksTokenSpec
QualifiedNameSpec: TypeAlias = Annotated[str, _validation.QUALIFIED_NAME_VALIDATOR]


IdentifierSpec: TypeAlias = Union[
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.IDENTIFIER_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.IDENTIFIER_PATTERN)],
]

WarehouseSpec: TypeAlias = Union[
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.WAREHOUSE_NAME_PATTERN)
    ],
    Annotated[
        Env, _api.EnvValueConstraints(pattern=_validation.WAREHOUSE_NAME_PATTERN)
    ],
]

WarehouseIdSpec: TypeAlias = Union[
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.WAREHOUSE_ID_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.WAREHOUSE_ID_PATTERN)],
]

VolumeSpec: TypeAlias = Union[
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.QUALIFIED_NAME_PATTERN)
    ],
    Annotated[
        Env, _api.EnvValueConstraints(pattern=_validation.QUALIFIED_NAME_PATTERN)
    ],
]
QueriesSpec: TypeAlias = list[str]
DatabricksTableNameSpec: TypeAlias = QualifiedNameSpec
DatabricksTableNamesSpec: TypeAlias = list[DatabricksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]


# --- Connection ------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_1, kw_only=True)
class DatabricksConn(Conn):
    host_url: HostUrlSpec
    token: CredentialsSpec
    volume: VolumeSpec
    warehouse: WarehouseSpec | None = None
    warehouse_id: WarehouseIdSpec | None = None
    catalog: IdentifierSpec | None = None
    schema: IdentifierSpec | None = None
    conn_cfg: CfgSpec | None = None

    def validate(self) -> None:
        if self.warehouse is not None and self.warehouse_id is not None:
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Conn",
                rule=f"warehouse={self.warehouse!r} and "
                f"warehouse_id={self.warehouse_id!r} are mutually exclusive",
            )
        if self.schema is not None and self.catalog is None:
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Conn",
                rule=f"schema={self.schema!r} requires catalog to be set",
            )


ConnSpec: TypeAlias = Union[NamedConn, DatabricksConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_3, kw_only=True)
class DatabricksSrc(Src):
    conn: ConnSpec
    queries: QueriesSpec
    initial_values: dict | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    src_cfg: CfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_4, kw_only=True)
class DatabricksDest(Dest):
    conn: ConnSpec
    tables: DatabricksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None
