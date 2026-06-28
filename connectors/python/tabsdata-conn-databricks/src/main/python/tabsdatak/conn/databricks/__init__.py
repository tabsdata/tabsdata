"""Databricks connector — public API.

* :class:`DatabricksConn` -- per-workspace connection (host_url,
  PAT token, warehouse, volume, catalog/schema).
* :class:`DatabricksSrc` -- source: list of SQL statements; one
  parquet slot per statement. Uses the Statement Execution API
  + external-link Arrow streams.
* :class:`DatabricksDest` -- destination: list of table FQNs;
  one parquet slot per table. Uses Volumes + ``COPY INTO``.

Token, host, identifier fields accept ``Secret``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Conn,
    ContextValueSpec,
    Dest,
    Secret,
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


HostUrlSpec: TypeAlias = Secret
DatabricksTokenSpec: TypeAlias = Secret
CredentialsSpec: TypeAlias = DatabricksTokenSpec


IdentifierSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.IDENTIFIER_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.IDENTIFIER_PATTERN)
    ],
]

WarehouseSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.WAREHOUSE_NAME_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.WAREHOUSE_NAME_PATTERN)
    ],
]

WarehouseIdSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.WAREHOUSE_ID_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.WAREHOUSE_ID_PATTERN)
    ],
]

VolumeSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.QUALIFIED_NAME_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.QUALIFIED_NAME_PATTERN)
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


def _validate_databricks_conn(conn) -> None:
    if conn.warehouse is not None and conn.warehouse_id is not None:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_2.exception(
            cls="Conn",
            rule=f"warehouse={conn.warehouse!r} and "
            f"warehouse_id={conn.warehouse_id!r} are mutually exclusive",
        )
    if conn.schema is not None and conn.catalog is None:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_2.exception(
            cls="Conn",
            rule=f"schema={conn.schema!r} requires catalog to be set",
        )


@_api.dataclass(DatabricksErrorCode.DATABRICKS_1, kw_only=True)
class DatabricksSrcConn(Conn):
    """Databricks workspace connection used by :class:`DatabricksSrc`."""

    __kind__ = "databricksSrcConn"

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
        _validate_databricks_conn(self)


@_api.dataclass(DatabricksErrorCode.DATABRICKS_1, kw_only=True)
class DatabricksDestConn(Conn):
    """Databricks workspace connection used by :class:`DatabricksDest`.

    Attribute-identical to :class:`DatabricksSrcConn` but a distinct
    class so source and destination connections cannot be confused
    at the wire-format level.
    """

    __kind__ = "databricksDestConn"

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
        _validate_databricks_conn(self)


# --- Source ----------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_3, kw_only=True)
class DatabricksSrc(Src):
    """Databricks source — one parquet slot per SQL statement.

    :param queries: List of SQL statements.
    :param initial_values: Optional ``{bind_name: value}`` dict.
        Each occurrence of ``{bind_name}`` in a query is substituted
        by the corresponding value at execute time (Python
        ``str.format``-style; values are inlined verbatim, not
        parameterised -- use only for literal-safe substitutions
        like dates and ids). Acts as the **first-run seed** for
        each bind name: at runtime, the plugin looks up
        ``ctx.get_attr(bind_name)`` per bind, and falls back to
        this field when the ctx scratch store has no entry for that
        name. User functions persist new values for the next run
        with ``ctx.store_attr(bind_name, new_value)``.
    :param src_cfg: Optional ``Mapping`` over
        ``databricks.logging_level``.
    """

    queries: QueriesSpec
    initial_values: dict[str, ContextValueSpec] | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_4, kw_only=True)
class DatabricksDest(Dest):
    """Databricks destination — one parquet slot per table.

    :param tables: Table names ([catalog.][schema.]table).
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"``.
    :param schema_evolution: ``"update"`` (default; forwards
        ``mergeSchema=true`` to ``COPY INTO``) or ``"strict"``
        (reject schema changes).
    :param dest_cfg: Optional dest cfg dict.
    """

    tables: DatabricksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    dest_cfg: DestCfgSpec | None = None
