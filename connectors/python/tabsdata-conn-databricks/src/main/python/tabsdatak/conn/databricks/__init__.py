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
  Arrow streams), one output slot per statement.
* `DatabricksDest` -- the `@subscriber` destination: a list of
  target `tables`, one output slot per table (staged to the Volume,
  then `COPY INTO`).
* `MergeCfg` -- how `DatabricksDest(if_table_exists="merge")` matches
  incoming rows onto the target table.

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
from tabsdatak.conn.common.sql._merge import MergeCfg as SqlMergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._watermark import validate_watermark_column

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
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
    StrOrSecretSpec,
    _validation.IDENTIFIER_VALIDATION,
]

WarehouseSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.WAREHOUSE_NAME_VALIDATION,
]

WarehouseIdSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.WAREHOUSE_ID_VALIDATION,
]

VolumeSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.QUALIFIED_NAME_VALIDATION,
]
QueriesSpec: TypeAlias = list[str]
DatabricksTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.DATABRICKS_TABLE_FQN_VALIDATION,
]
DatabricksTableNamesSpec: TypeAlias = list[DatabricksTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace", "merge"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]

# A column is a single Databricks identifier -- the `IdentifierSpec`
# shape, minus the FQN separator, so a dotted value is rejected instead of
# being read as a qualified reference.
DatabricksColumnSpec: TypeAlias = Annotated[str, _validation.COLUMN_VALIDATION]
# The merge spells the same shape under its own names, for the columns whose
# meaning is the merge's rather than the write's.
MergeColumnSpec: TypeAlias = DatabricksColumnSpec
MergeColumnsSpec: TypeAlias = list[DatabricksColumnSpec]


# --- DatabricksSrc.src_cfg keys -------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "databricks.logging_level",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- DatabricksDest.dest_cfg keys -----------------------------------
DestCfgKey: TypeAlias = Literal["databricks.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Merge -----------------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_18, kw_only=True)
class MergeCfg(_api.DescriptorBase):
    """How `if_table_exists="merge"` matches incoming rows onto a target table.

    :categories: destination

    The Databricks spelling of the shared `MergeCfg`. Columns are named as
    Databricks resolves them: case-insensitively, so `on=["order_id"]` hits
    `ORDER_ID` and `OrderId` alike. A backtick-quoted name is for a
    special-char or reserved name -- unlike Snowflake, quoting does not make it
    case-sensitive.

    Every incoming row carries a marker in `op_column` saying what to do with it,
    and the marker is what decides which clause claims the row:

    * `'i'` -- insert the row, if it matches no key.
    * `'u'` -- update the row it matches.
    * `'d'` -- delete the row it matches.

    The idempotency watermark is named on the destination rather than here --
    see `DatabricksDest.watermark_column`. It belongs to the write, not to the
    way one table's rows are matched, so a `DatabricksDest` naming a list of
    configs still stamps every one of its targets with the same column.

    Args:
        on: The key columns rows are matched by, joined with `AND`. Must be
            present on both sides. Each is a Databricks column identifier --
            bare, or backtick-quoted for a special-char name.
        op_column: The column holding each row's `i` / `u` / `d` marker. Read
            to decide each row's fate and never written, so the target does not
            need the column and will not gain it.

    Examples:
        A CDC feed whose `op` column says what to do with each row:

        .. code-block:: python

            MergeCfg(on=["order_id", "customer_id"], op_column="op")
    """

    __kind__ = "databricksMergeCfg"

    on: MergeColumnsSpec
    op_column: MergeColumnSpec

    def _fold(self, name: str) -> str:
        return _validation.merge_column_key(name)

    def _keys(self) -> list[str]:
        return [self._fold(name) for name in self.on]

    def _op_column(self) -> str:
        return self._fold(self.op_column)

    def _sql_merge_cfg(self) -> SqlMergeCfg:
        """This config in the dialect-neutral form the shared merge logic
        reads, with every name already folded to Databricks' comparison form."""
        return SqlMergeCfg(on=self._keys(), op_column=self._op_column())

    def validate(self) -> None:
        """Field validation, delegated to the shared config: `on` names at
        least one key and `op_column` does not collide with one of them.

        Called by the framework; raises `SqlCommonValidateException`.
        """
        self._sql_merge_cfg().validate()


MergeCfgSpec: TypeAlias = list[MergeCfg]
"""One `MergeCfg` per entry in `DatabricksDest.tables`, in the same order.

A list even for a single table, so that reading a merge config off a
destination never depends on how many tables it happens to write."""


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

    Field-identical to `DatabricksSrcConn` but a distinct class; source
    and destination connections are separate types and cannot be used in
    place of one another. See `DatabricksSrcConn` for the field
    meanings.
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
    """Databricks source for `@publisher` -- one output slot per query.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.sql.query`, the text of the query that
    produced the row, and `@td.sql.initial_values`, the JSON
    serialization of the bind values that query ran with (an empty JSON
    object when it has no binds). Setting the `src_cfg` key
    `tabsdata.src_metadata.drop` to `True` leaves them off.

    Args:
        queries: SQL statements, one output slot per entry, run through
            the Statement Execution API.
        initial_values: Optional `{bind_name: value}` seed dict. Each
            name binds into the query as a `:var` statement parameter
            (typed, native binding); it seeds the first run only, after
            which the plugin reads `ctx.get_attr(bind_name)` and the
            function persists new values with
            `ctx.set_attr(bind_name, ...)`.
        src_cfg: Optional connector config; supported keys are
            `databricks.logging_level` and
            `tabsdata.src_metadata.drop`.
    """

    # devguide-ref: source bind-value seeding & ctx scratch state

    queries: QueriesSpec
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Destination -----------------------------------------------------


@_api.dataclass(DatabricksErrorCode.DATABRICKS_4, kw_only=True)
class DatabricksDest(Dest):
    """Databricks destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is a
            1-, 2- or 3-part name (`table`, `schema.table` or
            `catalog.schema.table`); backtick-quote a part for special
            characters. Partial names are completed from the
            connection's `catalog` / `schema` defaults.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` overwrites the table.
            `"merge"` merges rows.
        schema_evolution: `"strict"` (the default) rejects schema changes;
            `"update"` adds new columns and performs type widening on existing
            columns of the target to match the schema of the incoming data.
        merge_cfg: Required by -- and only valid with --
            `if_table_exists="merge"`: how incoming rows are matched
            onto existing ones. A list, one entry per table in `tables` and
            in the same order -- a single table takes a one-entry list.
        watermark_column: An optional column stamped with the id of the trx the
            write belongs to, making the write idempotent: a rerun that finds
            the target already carrying this trx's id writes nothing rather than
            applying the same batch twice. Every table in `tables` is stamped in
            the same column. The target gains the column under either
            `schema_evolution` setting -- a watermark no row can be stamped in
            would do nothing at all -- and rows written before it existed keep a
            NULL there.

            A merge stamps through the `MERGE` itself; an append writes the
            column into the batch it stages, so `COPY INTO` stamps the rows it
            loads. A replace is refused it, having nothing to make idempotent --
            it rewrites the table wholesale, so a rerun already leaves the same
            rows behind.
        dest_cfg: Optional connector config; the only supported key is
            `databricks.logging_level`.

    Examples:
        Overwrite a Databricks table on each run:

        .. code-block:: python

            @subscriber(
                destination=DatabricksDest(
                    tables=["cap_replace"],
                    if_table_exists="replace",
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def publish(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Append rows and let `COPY INTO` add new columns via
        `schema_evolution`:

        .. code-block:: python

            @subscriber(
                destination=DatabricksDest(
                    tables=["cap_evolve"],
                    if_table_exists="append",
                    schema_evolution="update",
                ),
                input_tables=["caps_input/cap_evo_v2"],
            )
            def evolve(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Merge mode merges incoming rows onto the target by the keys in `merge_cfg`:

        .. code-block:: python
            @subscriber(
                destination=DatabricksDest(
                    tables=["cap_merge"],
                    if_table_exists="merge",
                    merge_cfg=[
                        MergeCfg(
                            on=["cap_id"],
                            op_column="op",
                        )
                    ],
                ),
                input_tables=["caps_input/cap_merge_v2"],
            )
            def merge(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    tables: DatabricksTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    merge_cfg: MergeCfgSpec | None = None
    watermark_column: DatabricksColumnSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def _merge_cfgs(self) -> list[MergeCfg] | None:
        """One `MergeCfg` per entry in `tables`, or `None` outside merge mode."""
        if self.merge_cfg is None:
            return None
        return list(self.merge_cfg)

    def _watermark_column(self) -> str | None:
        """The watermark column's stored form, or `None`. Folded the way a
        `MergeCfg` folds its own, so the two paths name one column."""
        if self.watermark_column is None:
            return None
        return _validation.merge_column_key(self.watermark_column)

    def validate(self) -> None:
        """Cross-field validation: `merge_cfg` and `if_table_exists="merge"`
        require each other, a list of configs must line up with `tables`, and
        `watermark_column` is not a replace's to name.

        Called by the framework; raises `DatabricksValidateException`.
        """
        merging = self.if_table_exists == "merge"
        if merging and self.merge_cfg is None:
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Dest",
                rule='if_table_exists="merge" requires merge_cfg to be set',
            )
        if not merging and self.merge_cfg is not None:
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Dest",
                rule=f"merge_cfg is set but if_table_exists="
                f"{self.if_table_exists!r}; merge_cfg only applies to"
                f' if_table_exists="merge"',
            )
        if self.merge_cfg is not None and len(self.merge_cfg) != len(self.tables):
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Dest",
                rule=f"merge_cfg has {len(self.merge_cfg)} entries but tables"
                f" has {len(self.tables)}; a list of merge configs must line up"
                f" one-to-one with tables (pass a single MergeCfg to apply one"
                f" config to every table)",
            )
        if self.watermark_column is not None and self.if_table_exists == "replace":
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_2.exception(
                cls="Dest",
                rule='watermark_column is set but if_table_exists="replace"; a'
                " replace rewrites the target wholesale, so rerunning one"
                " already leaves the same rows behind and there is nothing for"
                " a watermark to make idempotent",
            )
        for cfg in self._merge_cfgs() or []:
            # The watermark is named here, so the checks against what the merge
            # already reads are made here too.
            validate_watermark_column(cfg._sql_merge_cfg(), self._watermark_column())
