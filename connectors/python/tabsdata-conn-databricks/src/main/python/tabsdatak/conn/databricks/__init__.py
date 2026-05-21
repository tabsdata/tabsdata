"""Databricks connector — public API.

* :class:`DatabricksConn` -- per-workspace connection (host_url,
  PAT token, warehouse, volume, catalog/schema).
* :class:`DatabricksSrc` -- source: list of SQL statements; one
  parquet slot per statement. Uses the Statement Execution API
  + external-link Arrow streams.
* :class:`DatabricksDest` -- destination: list of table FQNs;
  one parquet slot per table. Uses Volumes + ``COPY INTO``.

Token, host, identifier fields accept ``Env`` / ``Secret``.
"""

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
    SecretOrEnvSpec,
    Src,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _validation
from tabsdatak.conn.databricks.error import DatabricksErrorCode

# --- Type aliases ----------------------------------------------------


HostUrlSpec: TypeAlias = SecretOrEnvSpec
DatabricksTokenSpec: TypeAlias = SecretOrEnvSpec
CredentialsSpec: TypeAlias = DatabricksTokenSpec


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
DatabricksTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.DATABRICKS_TABLE_FQN_VALIDATOR,
]
DatabricksTableNamesSpec: TypeAlias = list[DatabricksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]


# --- DatabricksSrc.src_cfg keys -------------------------------------
SrcCfgKey: TypeAlias = Literal["databricks.logging_level",]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- DatabricksDest.dest_cfg keys -----------------------------------
DestCfgKey: TypeAlias = Literal["databricks.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_1, kw_only=True)
class DatabricksConn(Conn):
    """Databricks workspace connection.

    :param host_url: Workspace URL
        (e.g. ``https://acme.cloud.databricks.com``).
    :param token: Personal access token (PAT). Stored as a
        ``Secret`` / ``Env``.
    :param volume: Unity Catalog volume used to stage parquet.
    :param warehouse: SQL warehouse name (mutually exclusive
        with ``warehouse_id``).
    :param warehouse_id: SQL warehouse id.
    :param catalog: Default catalog.
    :param schema: Default schema (requires ``catalog``).
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
    """

    host_url: HostUrlSpec
    token: CredentialsSpec
    volume: VolumeSpec
    warehouse: WarehouseSpec | None = None
    warehouse_id: WarehouseIdSpec | None = None
    catalog: IdentifierSpec | None = None
    schema: IdentifierSpec | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
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
    """Databricks source — one parquet slot per SQL statement.

    :param conn: :class:`DatabricksConn` (or :class:`NamedConn`).
    :param queries: List of SQL statements.
    :param initial_values: Optional ``{key: value}`` bind
        substituted as ``{key}`` placeholders in each query.
    :param src_cfg: Optional ``Mapping`` over
        ``databricks.logging_level``.
    """

    conn: ConnSpec
    queries: QueriesSpec
    initial_values: dict | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_4, kw_only=True)
class DatabricksDest(Dest):
    """Databricks destination — one parquet slot per table.

    :param conn: :class:`DatabricksConn` (or :class:`NamedConn`).
    :param tables: Table names ([catalog.][schema.]table).
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"``.
    :param schema_evolution: ``"update"`` (default; forwards
        ``mergeSchema=true`` to ``COPY INTO``) or ``"strict"``
        (reject schema changes).
    :param dest_cfg: Optional dest cfg dict.
    """

    conn: ConnSpec
    tables: DatabricksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    dest_cfg: DestCfgSpec | None = None
