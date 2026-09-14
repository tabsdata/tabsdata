#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake connector -- public API.

Three classes, imported from `tabsdatak.conn.snowflake`:

* `SnowflakeDestConn` -- the connection: a Snowflake `account`,
  `UserPassword` credentials, and optional default `role` /
  `database` / `schema` / `warehouse` / `stage`.
* `SnowflakeDest` -- the `@subscriber` destination: a list of
  target `tables`, one output slot per table (loaded with `PUT` +
  `COPY INTO` through a stage).
* `MergeCfg` -- how `SnowflakeDest(if_table_exists="merge")` matches
  incoming rows onto the target table.

Snowflake is destination-only -- there is no `SnowflakeSrc`. The
connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `SnowflakeDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=SnowflakeDest(tables=["cap_rows"]),
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
from tabsdatak.api import Dest, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg as SqlMergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _validation
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for Snowflake connections: `UserPassword`
(user + password / PAT)."""

AccountSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.ACCOUNT_VALIDATION,
]


# Snowflake identifier shape (unquoted or double-quoted), shared by
# both the dedicated identifier field (`stage`) and the Name fields
# (`role` / `database` / `schema` / `warehouse`) -- all four
# follow the same identifier rules.

IdentifierSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.IDENTIFIER_VALIDATION,
]

NameSpec: TypeAlias = IdentifierSpec
SnowflakeTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.SNOWFLAKE_TABLE_FQN_VALIDATION,
]
SnowflakeTableNamesSpec: TypeAlias = list[SnowflakeTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace", "merge"]

SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]

# A merge column is a single Snowflake identifier -- the `IdentifierSpec`
# shape, minus the FQN separator, so a dotted value is rejected instead of
# being read as a qualified reference.
MergeColumnSpec: TypeAlias = Annotated[str, _validation.MERGE_COLUMN_VALIDATOR]
MergeColumnsSpec: TypeAlias = list[MergeColumnSpec]


SchemaEvolutionEngineSpec: TypeAlias = Literal["native", "iceberg"]
"""Which machinery evolves the target's schema. `"native"` leans on
Snowflake's own `ENABLE_SCHEMA_EVOLUTION`, which `COPY INTO` honours;
`"iceberg"` has the connector diff the target against the incoming file and
emit the `ALTER` statements itself."""


# --- SnowflakeDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal[
    "snowflake.logging_level",
    "snowflake.schema_evolution_engine",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Merge -----------------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_11, kw_only=True)
class MergeCfg(SqlMergeCfg):
    """How `if_table_exists="merge"` matches incoming rows onto a target table.

    :categories: destination

    The Snowflake spelling of the shared `MergeCfg`: the connector stages the
    incoming parquet into a session-scoped temp table, then runs one `MERGE` per
    target table.

    Columns are named the way the target stores them: an unquoted name is
    upper-folded (so `on=["order_id"]` hits `ORDER_ID`), a double-quoted one is
    case-sensitive and taken verbatim.

    Every incoming row carries a marker in `op_column` saying what to do with it,
    and the marker is what decides which clause claims the row:

    * `'i'` -- insert the row, if it matches no key.
    * `'u'` -- update the row it matches.
    * `'d'` -- delete the row it matches.

    The marker is lower-cased before it is compared, so a feed emitting `'I'` /
    `'U'` / `'D'` works as well as one emitting lower case. Anything else matches
    no clause and the row is passed over.

    An update writes every column both sides share except the keys, which are
    what the row matched on; an insert writes those and the keys. Columns the
    target has and the incoming data does not keep their value on an update and
    take a NULL on an insert.

    Args:
        on: The key columns rows are matched by, joined with `AND`. Must be
            present on both sides. Each is a Snowflake column identifier --
            unquoted, or double-quoted for a case-sensitive / special-char name.
        op_column: The column holding each row's `i` / `u` / `d` marker. Read
            to decide each row's fate and never written, so the target does not
            need the column and will not gain it.
        watermark_column: An optional column stamped with the id of the trx the
            merge belongs to, making the merge idempotent: a rerun that finds
            the target already carrying this trx's id writes nothing rather than
            applying the same batch twice. The target gains the column under
            either `schema_evolution` setting -- a watermark no row can be
            stamped in would do nothing at all -- and rows written before it
            existed keep a NULL there.

    Examples:
        A CDC feed whose `op` column says what to do with each row:

        .. code-block:: python

            MergeCfg(on=["order_id", "customer_id"], op_column="op")
    """

    __kind__ = "snowflakeMergeCfg"

    # Re-annotated so the shared fields carry the Snowflake column shape: a
    # value that is not a Snowflake identifier is rejected here rather than
    # surfacing as invalid SQL when the MERGE is built.
    on: MergeColumnsSpec
    op_column: MergeColumnSpec
    watermark_column: MergeColumnSpec | None = None

    def _fold(self, name: str) -> str:
        return _validation.merge_column_key(name)


MergeCfgSpec: TypeAlias = MergeCfg | list[MergeCfg]
"""One `MergeCfg` applied to every target table, or one per entry in
`SnowflakeDest.tables`."""


# --- Connection ------------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_1, kw_only=True)
class SnowflakeDestConn(Conn):
    """Snowflake account connection.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or
    a `Secret`. `role` / `database` / `schema` / `warehouse`
    follow the same Snowflake identifier rules as `stage`.

    Args:
        account: A valid Snowflake account identifier / locator, e.g.
            `ABCDEFG-XY12345` (alphanumerics with dots, hyphens and
            underscores; starts and ends alphanumeric).
        credentials: `UserPassword` (user + password or PAT).
        role: Optional default role to `USE` before writing.
        database: Optional default database. Required when `schema`
            is set.
        schema: Optional default schema. Requires `database`.
        warehouse: Optional default warehouse; the plugin issues
            `USE WAREHOUSE` only when set.
        stage: Optional permanent stage for `PUT` + `COPY INTO`;
            when unset the plugin creates a session-scoped
            `TEMPORARY` stage. A Snowflake identifier -- unquoted, or
            double-quoted for case-sensitive / special-char names.
        conn_cfg: Optional connector-config dict (see `CfgSpec`),
            forwarded to snowflake-connector-python.
    """

    account: AccountSpec
    credentials: CredentialsSpec
    role: NameSpec | None = None
    database: NameSpec | None = None
    schema: NameSpec | None = None
    warehouse: NameSpec | None = None
    stage: IdentifierSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _account(self) -> str:
        return _convert(self.account, str)

    def _role(self) -> str:
        return _convert(self.role, str)

    def _database(self) -> str:
        return _convert(self.database, str)

    def _schema(self) -> str:
        return _convert(self.schema, str)

    def _warehouse(self) -> str:
        return _convert(self.warehouse, str)

    def _stage(self) -> str:
        return _convert(self.stage, str)

    def validate(self) -> None:
        """Cross-field validation: `schema` requires `database`.

        Called by the framework; raises `SnowflakeValidateException`.
        """
        if self.schema is not None and self.database is None:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_2.exception(schema=self.schema)


# --- Destination -----------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_3, kw_only=True)
class SnowflakeDest(Dest):
    """Snowflake destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is a
            1-, 2- or 3-part name (`table`, `schema.table` or
            `database.schema.table`); double-quote a part for
            case-sensitive / special-char names.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` issues `TRUNCATE` before `COPY INTO` (so
            the existing schema is preserved);
            `"merge"` stages the rows and `MERGE`s them onto the
            target by the keys in `merge_cfg`, updating, deleting or
            inserting each one.
        schema_evolution: `"strict"` (the default) rejects schema changes;
            `"update"` adds new columns to the target to match the schema of
            the incoming data.
        merge_cfg: Required by -- and only valid with --
            `if_table_exists="merge"`: how incoming rows are matched
            onto existing ones. One `MergeCfg` applies to every table;
            pass a list to configure each table in `tables` separately.
        dest_cfg: Optional connector config. Supported keys:
            `snowflake.logging_level`, and
            `snowflake.schema_evolution_engine` -- `"native"` (the default
            when unset) or `"iceberg"`. Native schema evolution is performed with
            Snowflake's
            `ENABLE_SCHEMA_EVOLUTION`, which `COPY INTO` honours and which
            only ever adds a column. `"iceberg"` diffs the
            target against the incoming file instead, which also widens a
            column whose type the batch has outgrown and refuses a change
            Snowflake could not make. It applies only where the schema is
            allowed to move, so it does nothing under
            `schema_evolution="strict"`; `if_table_exists="merge"` requires
            `snowflake.schema_evolution_engine="iceberg"`, since a `MERGE` does not read
            the native mark at all.

    Examples:
        Append rows to a table -- the default disposition, which
        accumulates across runs:

        .. code-block:: python

            @subscriber(
                destination=SnowflakeDest(
                    tables=["cap_append"],
                    if_table_exists="append",
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def load_append(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Replace mode truncates the target before `COPY INTO`,
        preserving the existing schema on reload:

        .. code-block:: python

            @subscriber(
                destination=SnowflakeDest(
                    tables=["cap_replace"],
                    if_table_exists="replace",
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def load_replace(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Merge mode upserts by key instead of accumulating, and applies
        the CDC tombstones the incoming `op` column marks:

        .. code-block:: python

            @subscriber(
                destination=SnowflakeDest(
                    tables=["cap_merge"],
                    if_table_exists="merge",
                    merge_cfg=MergeCfg(on=["cap_id"], op_column="op"),
                    dest_cfg={"snowflake.schema_evolution_engine": "iceberg"},
                ),
                input_tables=["caps_input/cap_changes"],
            )
            def load_merge(changes: TableFrameSpec) -> TableFrameSpec:
                return changes
    """

    tables: SnowflakeTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    merge_cfg: MergeCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def _merge_cfgs(self) -> list[MergeCfg] | None:
        """One `MergeCfg` per entry in `tables`, or `None` outside merge mode."""
        if self.merge_cfg is None:
            return None
        if isinstance(self.merge_cfg, MergeCfg):
            return [self.merge_cfg] * len(self.tables)
        return list(self.merge_cfg)

    def validate(self) -> None:
        """Cross-field validation: `merge_cfg` and `if_table_exists="merge"`
        require each other, a list of configs must line up with `tables`, and
        merging requires `schema_evolution_engine="iceberg"`.

        Called by the framework; raises `SnowflakeValidateException`.
        """
        merging = self.if_table_exists == "merge"
        if merging and self.merge_cfg is None:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_12.exception()
        if not merging and self.merge_cfg is not None:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_13.exception(
                if_table_exists=self.if_table_exists
            )
        if isinstance(self.merge_cfg, list) and len(self.merge_cfg) != len(self.tables):
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_14.exception(
                cfgs=len(self.merge_cfg), tables=len(self.tables)
            )
        # Last, so a merge that is malformed in a more basic way reports that
        # instead: a missing merge_cfg is the thing to fix first.
        engine = (self.dest_cfg or {}).get("snowflake.schema_evolution_engine")
        if merging and engine != "iceberg":
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_22.exception()
