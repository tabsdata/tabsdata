#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks connector -- public API.

Four classes, imported from `tabsdatak.conn.databricks`:

* `DatabricksSrcConn` / `DatabricksDestConn` -- the connection: a
  workspace `host_url` and PAT `token` (both bare `Secret`), a
  staging `volume`, an optional `warehouse` / `warehouse_id` and
  default `catalog` / `schema`.
* `DatabricksSrc` -- the `@publisher` source: a list of SQL
  `queries` run through the Statement Execution API (external-link
  Arrow streams), one parquet slot per statement.
* `DatabricksDest` -- the `@subscriber` destination: a list of
  target `tables`, one parquet slot per table (staged to the Volume,
  then `COPY INTO`).

The connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `DatabricksSrc` / `DatabricksDest`.

Examples:
    .. code-block:: python

        @publisher(source=DatabricksSrc(queries=["SELECT * FROM wld_city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=DatabricksDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    Secret,
    Src,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    StrOrSecretConstraints,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _validation
from tabsdatak.conn.databricks.error import DatabricksErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


HostUrlSpec: TypeAlias = Secret
DatabricksTokenSpec: TypeAlias = Secret
CredentialsSpec: TypeAlias = DatabricksTokenSpec
"""The credential type for Databricks connections: a bare `Secret`
holding the personal access token (PAT)."""


IdentifierSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.IDENTIFIER_PATTERN)
]

WarehouseSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.WAREHOUSE_NAME_PATTERN)
]

WarehouseIdSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.WAREHOUSE_ID_PATTERN)
]

VolumeSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.QUALIFIED_NAME_PATTERN)
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
    """Databricks workspace connection used by `DatabricksSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. `host_url` and `token` are bare
    `Secret` values; the identifier fields accept a plain string or a
    `Secret`.

    Args:
        host_url: The workspace URL (a `Secret`), e.g.
            `https://<workspace>.cloud.databricks.com`.
        token: The personal access token (PAT), a `Secret`.
        volume: A Unity Catalog Volume used to stage parquet, given as a
            `[catalog.][schema.]volume` name (backtick-quote a part
            for special characters).
        warehouse: Optional SQL warehouse name (1-100 characters, no
            leading / trailing whitespace). Mutually exclusive with
            `warehouse_id`.
        warehouse_id: Optional SQL warehouse id (16-character lowercase
            hex). Mutually exclusive with `warehouse`.
        catalog: Optional default catalog (a Databricks identifier).
        schema: Optional default schema (a Databricks identifier);
            requires `catalog`.
        entity_whitelist: Optional list of regex patterns narrowing
            which tables the source considers; `None` (the default)
            filters nothing.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
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

    def _volume(self) -> str:
        return _convert(self.volume, str)

    def _warehouse(self) -> str:
        return _convert(self.warehouse, str)

    def _warehouse_id(self) -> str:
        return _convert(self.warehouse_id, str)

    def _catalog(self) -> str:
        return _convert(self.catalog, str)

    def _schema(self) -> str:
        return _convert(self.schema, str)

    def validate(self) -> None:
        """Cross-field validation: `warehouse` and `warehouse_id` are
        mutually exclusive, and `schema` requires `catalog`.

        Called by the framework; raises `DatabricksValidateException`.
        """
        _validate_databricks_conn(self)


@_api.dataclass(DatabricksErrorCode.DATABRICKS_1, kw_only=True)
class DatabricksDestConn(Conn):
    """Databricks workspace connection used by `DatabricksDest`.

    :categories: destination

    Field-identical to `DatabricksSrcConn` but a distinct class so
    source and destination connections cannot be confused at the
    wire-format level. See `DatabricksSrcConn` for the field meanings.
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

    def _volume(self) -> str:
        return _convert(self.volume, str)

    def _warehouse(self) -> str:
        return _convert(self.warehouse, str)

    def _warehouse_id(self) -> str:
        return _convert(self.warehouse_id, str)

    def _catalog(self) -> str:
        return _convert(self.catalog, str)

    def _schema(self) -> str:
        return _convert(self.schema, str)

    def validate(self) -> None:
        """Cross-field validation: `warehouse` and `warehouse_id` are
        mutually exclusive, and `schema` requires `catalog`.

        Called by the framework; raises `DatabricksValidateException`.
        """
        _validate_databricks_conn(self)


# --- Source ----------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_3, kw_only=True)
class DatabricksSrc(Src):
    """Databricks source for `@publisher` -- one parquet slot per query.

    :categories: source

    Args:
        queries: SQL statements, one output slot per entry, run through
            the Statement Execution API.
        initial_values: Optional `{bind_name: value}` seed dict. Each
            name binds into the query as a `:var` statement parameter
            (typed, native binding); it seeds the first run only, after
            which the plugin reads `ctx.get_attr(bind_name)` and the
            function persists new values with
            `ctx.set_attr(bind_name, ...)`.
        src_cfg: Optional connector config; the only supported key is
            `databricks.logging_level`.
    """

    # devguide-ref: source bind-value seeding & ctx scratch state

    queries: QueriesSpec
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_4, kw_only=True)
class DatabricksDest(Dest):
    """Databricks destination for `@subscriber` -- one parquet slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is a
            1-, 2- or 3-part name (`table`, `schema.table` or
            `catalog.schema.table`); backtick-quote a part for special
            characters. Partial names are completed from the
            connection's `catalog` / `schema` defaults.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` overwrites the table.
        schema_evolution: `"update"` (the default) forwards
            `mergeSchema=true` to `COPY INTO` so new columns are
            added; `"strict"` rejects schema changes.
        dest_cfg: Optional connector config; the only supported key is
            `databricks.logging_level`.
    """

    tables: DatabricksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    dest_cfg: DestCfgSpec | None = None
