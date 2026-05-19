"""ClickHouse-specific Iceberg schema-evolution renderer + helpers.

The dialect-agnostic infrastructure (models, delta core,
:class:`DialectRenderer` abstract base) lives in
``tabsdatak.conn.common.sql._iceberg``; this module ships only the
ClickHouse-specific pieces consumed by ``_dialect.py``.
"""

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._iceberg import (
    Action,
    ColumnDef,
    DialectRenderer,
    RenderOptions,
    ReorderPolicy,
    SchemaChange,
    SchemaDelta,
    TableSchema,
    TypeWideningMap,
    UnsafeSchemaChangeError,
    compute_delta,
    validate_delta,
)


__all__ = [
    # Re-exported for callers that import all iceberg pieces from a
    # single dialect module.
    "Action",
    "ColumnDef",
    "DialectRenderer",
    "RenderOptions",
    "ReorderPolicy",
    "SchemaChange",
    "SchemaDelta",
    "TableSchema",
    "TypeWideningMap",
    "UnsafeSchemaChangeError",
    "compute_delta",
    "validate_delta",
    # ClickHouse-specific surface.
    "CLICKHOUSE_TYPE_WIDENING_MAP",
    "ClickHouseRenderer",
    "parse_clickhouse_type",
    "render_clickhouse_sql",
    "table_schema_from_arrow",
    "table_schema_from_system_columns",
]


# --- ClickHouse renderer ---------------------------------------------


class ClickHouseRenderer(DialectRenderer):
    SUPPORTED_ACTIONS = {
        Action.ADD_COLUMN,
        Action.DROP_COLUMN,
        Action.ALTER_TYPE,
        Action.SET_NULLABLE,
        Action.SET_DEFAULT,
        Action.DROP_DEFAULT,
        Action.REORDER_COLUMN,
        # DROP_NULLABLE not supported by ClickHouse.
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
                f"Nullable({d['type']})"
                if d.get("nullable", True)
                else d["type"]
            )
            stmt = (
                f"ALTER TABLE {t} ADD COLUMN "
                f"`{change.column}` {effective_type}"
            )
            if d.get("default") is not None:
                stmt += f" DEFAULT {d['default']}"
            return stmt

        if change.action == Action.DROP_COLUMN:
            return f"ALTER TABLE {t} DROP COLUMN `{change.column}`"

        if change.action == Action.ALTER_TYPE:
            new_type = d["new_type"]
            if d.get("nullable", False):
                new_type = f"Nullable({new_type})"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {new_type}"
            )

        if change.action == Action.SET_NULLABLE:
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` Nullable({d['type']})"
            )

        if change.action == Action.SET_DEFAULT:
            effective_type = d["type"]
            if d.get("nullable", False):
                effective_type = f"Nullable({effective_type})"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {effective_type} "
                f"DEFAULT {d['new_default']}"
            )

        if change.action == Action.DROP_DEFAULT:
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` REMOVE DEFAULT"
            )

        if change.action == Action.REORDER_COLUMN:
            effective_type = (
                f"Nullable({d['type']})"
                if d.get("nullable", True)
                else d["type"]
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

    def _create_table_sql(
        self, table_name: str, col_defs: str
    ) -> str:
        return (
            f"CREATE TABLE {table_name} (\n    {col_defs}\n) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )


# --- Type widening map -----------------------------------------------

CLICKHOUSE_TYPE_WIDENING_MAP: TypeWideningMap = {
    "Int16": {"Int8"},
    "Int32": {"Int8", "Int16"},
    "Int64": {"Int8", "Int16", "Int32"},
    "UInt16": {"UInt8"},
    "UInt32": {"UInt8", "UInt16"},
    "UInt64": {"UInt8", "UInt16", "UInt32"},
    "Float64": {"Float32"},
}


# --- Type normalization ----------------------------------------------


def parse_clickhouse_type(type_str: str) -> tuple[str, bool]:
    """Parse a ClickHouse type string into ``(base_type, nullable)``.

    Unwraps ``Nullable()`` and ``LowCardinality()`` wrappers; the
    ``nullable`` flag is True if any wrapper layer is ``Nullable``.
    """
    type_str = type_str.strip()
    nullable = False

    changed = True
    while changed:
        changed = False
        if type_str.startswith("Nullable(") and type_str.endswith(")"):
            type_str = type_str[len("Nullable("):-1]
            nullable = True
            changed = True
        if (
            type_str.startswith("LowCardinality(")
            and type_str.endswith(")")
        ):
            type_str = type_str[len("LowCardinality("):-1]
            changed = True

    return type_str, nullable


def _parse_default(
    default_kind: str | None,
    default_expression: str | None,
) -> str | None:
    if default_kind == "DEFAULT" and default_expression:
        return default_expression
    return None


# --- Schema conversion -----------------------------------------------


def table_schema_from_system_columns(
    table_name: str,
    rows: list[tuple[str, str, str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` from ``system.columns`` query
    results: rows are ``(name, type, default_kind, default_expression)``.
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
    """Build a :class:`TableSchema` from a PyArrow schema using
    ClickHouse types. Container types (Array/Map/Tuple) are treated as
    non-nullable since ClickHouse doesn't support top-level ``Nullable``
    on those.
    """
    import pyarrow as pa

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._dialect import (
        _arrow_type_to_clickhouse,
    )

    container_checks = (
        pa.types.is_list,
        pa.types.is_large_list,
        pa.types.is_map,
        pa.types.is_struct,
    )

    columns = []
    for field in arrow_schema:
        ch_type = _arrow_type_to_clickhouse(field.type)
        is_container = any(
            check(field.type) for check in container_checks
        )
        if is_container:
            nullable = False
        elif (
            column_nulls is not None and field.name in column_nulls
        ):
            nullable = column_nulls[field.name] is not False
        else:
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


def render_clickhouse_sql(
    delta: SchemaDelta,
    options: RenderOptions | None = None,
) -> list[str]:
    """Render a SchemaDelta as ClickHouse SQL statements."""
    return ClickHouseRenderer().render_sql(delta, options)
