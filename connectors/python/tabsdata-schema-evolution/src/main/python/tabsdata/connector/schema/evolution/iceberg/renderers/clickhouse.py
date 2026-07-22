#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse-specific schema evolution: renderer, type helpers, and schema conversion.

This module provides:

- :class:`ClickHouseRenderer` — renders :class:`SchemaDelta` as ClickHouse DDL.
- Type normalization: parsing ``Nullable(T)`` wrappers from ``system.columns``.
- Schema conversion: building :class:`TableSchema` from ``system.columns``
  rows or from a PyArrow schema.
- A canonical type-widening map for ClickHouse integer and float families.
"""

from __future__ import annotations

from tabsdata.connector.schema.evolution.iceberg.models import (
    Action,
    ColumnDef,
    RenderOptions,
    SchemaChange,
    SchemaDelta,
    TableSchema,
    TypeWideningMap,
)
from tabsdata.connector.schema.evolution.iceberg.renderers.base import DialectRenderer

# ── Renderer ──────────────────────────────────────────────────────────────────────────


class ClickHouseRenderer(DialectRenderer):
    SUPPORTED_ACTIONS = {
        Action.ADD_COLUMN,
        Action.DROP_COLUMN,
        Action.ALTER_TYPE,
        Action.SET_NULLABLE,
        Action.SET_DEFAULT,
        Action.DROP_DEFAULT,
        Action.REORDER_COLUMN,
        # DROP_NULLABLE not supported by ClickHouse
    }
    SUPPORTS_REORDER = True

    def column_def(
        self,
        name: str,
        type_: str,
        nullable: bool,
        default: str | None,
    ) -> str:
        effective_type = f"Nullable({type_})" if nullable else type_
        col = f"`{name}` {effective_type}"
        if default is not None:
            col += f" DEFAULT {default}"
        return col

    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        d = change.detail
        t = table_name

        if change.action == Action.ADD_COLUMN:
            effective_type = (
                f"Nullable({d['type']})" if d.get("nullable", True) else d["type"]
            )
            stmt = f"ALTER TABLE {t} ADD COLUMN `{change.column}` {effective_type}"
            if d.get("default") is not None:
                stmt += f" DEFAULT {d['default']}"
            return stmt

        if change.action == Action.DROP_COLUMN:
            return f"ALTER TABLE {t} DROP COLUMN `{change.column}`"

        if change.action == Action.ALTER_TYPE:
            new_type = d["new_type"]
            if d.get("nullable", False):
                new_type = f"Nullable({new_type})"
            return f"ALTER TABLE {t} MODIFY COLUMN `{change.column}` {new_type}"

        if change.action == Action.SET_NULLABLE:
            return (
                f"ALTER TABLE {t} MODIFY COLUMN `{change.column}` Nullable({d['type']})"
            )

        if change.action == Action.SET_DEFAULT:
            effective_type = d["type"]
            if d.get("nullable", False):
                effective_type = f"Nullable({effective_type})"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {effective_type} DEFAULT {d['new_default']}"
            )

        if change.action == Action.DROP_DEFAULT:
            return f"ALTER TABLE {t} MODIFY COLUMN `{change.column}` REMOVE DEFAULT"

        if change.action == Action.REORDER_COLUMN:
            effective_type = (
                f"Nullable({d['type']})" if d.get("nullable", True) else d["type"]
            )
            after = d.get("after_column")
            if after is None:
                return (
                    f"ALTER TABLE {t} MODIFY COLUMN "
                    f"`{change.column}` {effective_type} FIRST"
                )
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {effective_type} AFTER `{after}`"
            )

        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        return (
            f"CREATE TABLE {table_name} (\n    {col_defs}\n) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )


# ── Type widening map ─────────────────────────────────────────────────────────────────

CLICKHOUSE_TYPE_WIDENING_MAP: TypeWideningMap = {
    "Int16": {"Int8"},
    "Int32": {"Int8", "Int16"},
    "Int64": {"Int8", "Int16", "Int32"},
    "UInt16": {"UInt8"},
    "UInt32": {"UInt8", "UInt16"},
    "UInt64": {"UInt8", "UInt16", "UInt32"},
    "Float64": {"Float32"},
}


# ── Type normalization ────────────────────────────────────────────────────────────────


def parse_clickhouse_type(type_str: str) -> tuple[str, bool]:
    """Parse a ClickHouse type string into ``(base_type, nullable)``.

    Unwraps ``Nullable()`` and ``LowCardinality()`` wrappers to extract the
    base type. The ``nullable`` flag is ``True`` only if the outermost or
    any intermediate wrapper is ``Nullable``.

    Examples::

        parse_clickhouse_type("Nullable(Int32)")      → ("Int32", True)
        parse_clickhouse_type("Int64")                 → ("Int64", False)
        parse_clickhouse_type("LowCardinality(String)") → ("String", False)
        parse_clickhouse_type("Nullable(LowCardinality(String))")
                                                       → ("String", True)
    """
    type_str = type_str.strip()
    nullable = False

    # Iteratively unwrap Nullable() and LowCardinality()
    changed = True
    while changed:
        changed = False
        if type_str.startswith("Nullable(") and type_str.endswith(")"):
            type_str = type_str[len("Nullable(") : -1]
            nullable = True
            changed = True
        if type_str.startswith("LowCardinality(") and type_str.endswith(")"):
            type_str = type_str[len("LowCardinality(") : -1]
            changed = True

    return type_str, nullable


def _parse_default(
    default_kind: str | None,
    default_expression: str | None,
) -> str | None:
    """Convert ClickHouse ``default_kind``/``default_expression`` to a default string.

    ClickHouse ``system.columns`` reports ``default_kind`` as one of:
    ``''`` (no default), ``'DEFAULT'``, ``'MATERIALIZED'``, ``'ALIAS'``, or
    ``'EPHEMERAL'``.

    Only ``DEFAULT`` is relevant for schema evolution; others are ignored.
    """
    if default_kind == "DEFAULT" and default_expression:
        return default_expression
    return None


# ── Schema conversion ─────────────────────────────────────────────────────────────────


def table_schema_from_system_columns(
    table_name: str,
    rows: list[tuple[str, str, str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` from ``system.columns`` query results.

    Args:
        table_name: Fully qualified table name (e.g. ``"db.my_table"``).
        rows: Result rows from::

            SELECT name, type, default_kind, default_expression
            FROM system.columns
            WHERE database = ... AND table = ...
            ORDER BY position

          Each row is ``(name, type, default_kind, default_expression)``.

    Returns:
        A TableSchema with normalized ClickHouse types.
    """
    columns = []
    for name, type_str, default_kind, default_expression in rows:
        base_type, nullable = parse_clickhouse_type(type_str)
        default = _parse_default(default_kind, default_expression)
        columns.append(
            ColumnDef(
                name=name,
                type=base_type,
                nullable=nullable,
                default=default,
            )
        )
    return TableSchema(table_name=table_name, columns=columns)


def table_schema_from_arrow(
    table_name: str,
    arrow_schema,
    column_nulls: dict[str, bool | None] | None = None,
) -> TableSchema:
    """Build a :class:`TableSchema` from a PyArrow schema using ClickHouse types.

    Uses the same type conversion logic as the ClickHouse dialect's
    ``_arrow_type_to_clickhouse``. Container types (Array, Map, Tuple) are
    treated as non-nullable since ClickHouse does not support top-level
    ``Nullable`` on those types.

    Args:
        table_name: Table name for the resulting schema.
        arrow_schema: A ``pyarrow.Schema`` instance.
        column_nulls: Per-column nullability derived from Parquet file
            statistics.  Maps column names to ``True`` (data contains
            nulls), ``False`` (no nulls in data), or ``None`` (stats
            unavailable).  When provided, this overrides the Arrow
            schema-level ``nullable`` flag — which is always ``True``
            for Polars-produced files — giving an accurate picture of
            whether the data actually requires nullable columns.
            Columns with ``None`` (stats unavailable) are treated as
            nullable to be safe.

    Returns:
        A TableSchema with ClickHouse type tokens.
    """
    try:
        import pyarrow as pa

        from tabsdata._io.cloud_staging.clickhouse.dialect import (
            _arrow_type_to_clickhouse,
        )
    except ImportError as e:
        raise ImportError(
            "pyarrow and the ClickHouse dialect are required for "
            "Arrow-to-TableSchema conversion."
        ) from e

    container_checks = (
        pa.types.is_list,
        pa.types.is_large_list,
        pa.types.is_map,
        pa.types.is_struct,
    )

    columns = []
    for field in arrow_schema:
        ch_type = _arrow_type_to_clickhouse(field.type)
        is_container = any(check(field.type) for check in container_checks)
        if is_container:
            nullable = False
        elif column_nulls is not None and field.name in column_nulls:
            # Use Parquet stats: True or None (missing stats) → nullable,
            # False → not nullable
            nullable = column_nulls[field.name] is not False
        else:
            # Fall back to Arrow schema flag
            nullable = field.nullable
        columns.append(
            ColumnDef(
                name=field.name,
                type=ch_type,
                nullable=nullable,
                default=None,
            )
        )
    return TableSchema(table_name=table_name, columns=columns)


# ── SQL rendering convenience ─────────────────────────────────────────────────────────


def render_clickhouse_sql(
    delta: SchemaDelta,
    options: RenderOptions | None = None,
) -> list[str]:
    """Render a SchemaDelta as ClickHouse SQL statements.

    Convenience wrapper around ``render_sql(delta, "clickhouse", options)``.
    """
    from tabsdata.connector.schema.evolution.iceberg.renderers import render_sql

    return render_sql(delta, "clickhouse", options)
