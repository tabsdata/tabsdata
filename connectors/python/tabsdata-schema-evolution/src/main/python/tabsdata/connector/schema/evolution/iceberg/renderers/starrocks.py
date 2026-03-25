#
# Copyright 2026 Tabs Data Inc.
#

"""StarRocks-specific schema evolution: renderer, type helpers, and schema conversion.

This module provides:

- :class:`StarRocksRenderer` — renders :class:`SchemaDelta` as StarRocks DDL.
- Type normalization: parsing MySQL-format type strings from
  ``information_schema.COLUMNS`` into canonical StarRocks DDL types.
- Schema conversion: building :class:`TableSchema` from ``information_schema``
  rows or from a PyArrow schema.
- A canonical type-widening map for StarRocks integer and float families.
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


class StarRocksRenderer(DialectRenderer):
    """Render :class:`SchemaDelta` as StarRocks-compatible DDL statements.

    StarRocks uses ``NULL``/``NOT NULL`` keywords for nullability (no
    ``Nullable()`` wrapper) and backtick quoting for identifiers.

    Unsupported operations:

    - ``DROP_NULLABLE`` — StarRocks does not support making a nullable column
      non-nullable.
    - ``REORDER_COLUMN`` — StarRocks uses ``ORDER BY`` for column ordering
      which is not a per-column operation; skipped for now.
    """

    SUPPORTED_ACTIONS = {
        Action.ADD_COLUMN,
        Action.DROP_COLUMN,
        Action.ALTER_TYPE,
        Action.SET_NULLABLE,
        Action.SET_DEFAULT,
        Action.DROP_DEFAULT,
        # DROP_NULLABLE not supported by StarRocks
        # REORDER_COLUMN not supported (StarRocks uses ORDER BY, not per-column AFTER)
    }
    SUPPORTS_REORDER = False

    def column_def(
        self,
        name: str,
        type_: str,
        nullable: bool,
        default: str | None,
    ) -> str:
        """Render a column definition for CREATE TABLE.

        Format: ```name` TYPE NULL|NOT NULL [DEFAULT x]``
        """
        null_clause = "NULL" if nullable else "NOT NULL"
        col = f"`{name}` {type_} {null_clause}"
        if default is not None:
            col += f" DEFAULT {default}"
        return col

    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        """Render a single ALTER TABLE statement for StarRocks.

        Returns ``None`` for unsupported actions.
        """
        d = change.detail
        t = table_name

        if change.action == Action.ADD_COLUMN:
            null_clause = "NULL" if d.get("nullable", True) else "NOT NULL"
            stmt = (
                f"ALTER TABLE {t} ADD COLUMN "
                f"`{change.column}` {d['type']} {null_clause}"
            )
            if d.get("default") is not None:
                stmt += f" DEFAULT {d['default']}"
            return stmt

        if change.action == Action.DROP_COLUMN:
            return f"ALTER TABLE {t} DROP COLUMN `{change.column}`"

        if change.action == Action.ALTER_TYPE:
            new_type = d["new_type"]
            null_clause = "NULL" if d.get("nullable", False) else "NOT NULL"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {new_type} {null_clause}"
            )

        if change.action == Action.SET_NULLABLE:
            return f"ALTER TABLE {t} MODIFY COLUMN `{change.column}` {d['type']} NULL"

        if change.action == Action.SET_DEFAULT:
            null_clause = "NULL" if d.get("nullable", False) else "NOT NULL"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {d['type']} {null_clause} "
                f"DEFAULT {d['new_default']}"
            )

        if change.action == Action.DROP_DEFAULT:
            null_clause = "NULL" if d.get("nullable", False) else "NOT NULL"
            return (
                f"ALTER TABLE {t} MODIFY COLUMN "
                f"`{change.column}` {d['type']} {null_clause}"
            )

        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        """Render a CREATE TABLE statement without engine suffix.

        StarRocks table properties (DUPLICATE KEY, DISTRIBUTED BY, PROPERTIES)
        are caller-specific, so only the column definitions are included here.
        """
        return f"CREATE TABLE {table_name} (\n    {col_defs}\n)"


# ── Type widening map ─────────────────────────────────────────────────────────────────

STARROCKS_TYPE_WIDENING_MAP: TypeWideningMap = {
    "SMALLINT": {"TINYINT"},
    "INT": {"TINYINT", "SMALLINT"},
    "BIGINT": {"TINYINT", "SMALLINT", "INT"},
    "LARGEINT": {"TINYINT", "SMALLINT", "INT", "BIGINT"},
    "DOUBLE": {"FLOAT"},
}


# ── Type normalization ────────────────────────────────────────────────────────────────


def parse_starrocks_type(type_str: str) -> tuple[str, bool]:
    """Parse a StarRocks type string into ``(base_type, nullable)``.

    StarRocks stores types in ``information_schema.COLUMNS`` using MySQL-format
    names such as ``int(11)``, ``bigint(20)``, ``varchar(65533)``, and
    ``tinyint(1)`` (for BOOLEAN).  This function normalizes those to canonical
    DDL type names.

    Unlike ClickHouse, StarRocks does not encode nullability in the type string
    — nullability comes from the ``IS_NULLABLE`` column in ``information_schema``.
    The ``nullable`` component of the return value is therefore always ``False``;
    callers should read ``IS_NULLABLE`` separately.

    Examples::

        parse_starrocks_type("int(11)")       → ("INT", False)
        parse_starrocks_type("bigint(20)")    → ("BIGINT", False)
        parse_starrocks_type("tinyint(1)")    → ("BOOLEAN", False)
        parse_starrocks_type("varchar(65533)") → ("STRING", False)
        parse_starrocks_type("DOUBLE")         → ("DOUBLE", False)

    Returns:
        A tuple of ``(base_type, nullable)`` where ``nullable`` is always
        ``False`` (nullability must be determined from ``IS_NULLABLE``).
    """
    from tabsdata._io.cloud_staging.starrocks.dialect import _normalize_column_type

    return _normalize_column_type(type_str.strip()), False


# ── Schema conversion ─────────────────────────────────────────────────────────────────


def table_schema_from_information_schema(
    table_name: str,
    rows: list[tuple[str, str, str, str | None]],
) -> TableSchema:
    """Build a :class:`TableSchema` from ``information_schema.COLUMNS`` query results.

    Args:
        table_name: Fully qualified table name (e.g. ``"db.my_table"``).
        rows: Result rows from::

            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION

          Each row is ``(column_name, column_type, is_nullable, column_default)``.
          ``is_nullable`` is ``"YES"`` or ``"NO"``.
          ``column_default`` is the default expression string or ``None``.

    Returns:
        A TableSchema with normalized StarRocks types.
    """
    from tabsdata._io.cloud_staging.starrocks.dialect import _normalize_column_type

    columns = []
    for column_name, column_type, is_nullable, column_default in rows:
        base_type = _normalize_column_type(column_type.strip())
        nullable = is_nullable == "YES"
        default = column_default if column_default is not None else None
        columns.append(
            ColumnDef(
                name=column_name,
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
    """Build a :class:`TableSchema` from a PyArrow schema using StarRocks types.

    Uses the same type conversion logic as the StarRocks dialect's
    ``_arrow_type_to_starrocks``. Container types (Array, Map, Struct) are
    treated as non-nullable since StarRocks does not support top-level
    ``NULL`` on those types.

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
        A TableSchema with StarRocks type tokens.
    """
    try:
        import pyarrow as pa

        from tabsdata._io.cloud_staging.starrocks.dialect import (
            _arrow_type_to_starrocks,
        )
    except ImportError as e:
        raise ImportError(
            "pyarrow and the StarRocks dialect are required for "
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
        sr_type = _arrow_type_to_starrocks(field.type)
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
                type=sr_type,
                nullable=nullable,
                default=None,
            )
        )
    return TableSchema(table_name=table_name, columns=columns)


# ── SQL rendering convenience ─────────────────────────────────────────────────────────


def render_starrocks_sql(
    delta: SchemaDelta,
    options: RenderOptions | None = None,
) -> list[str]:
    """Render a SchemaDelta as StarRocks SQL statements.

    Convenience wrapper around ``render_sql(delta, "starrocks", options)``.
    """
    from tabsdata.connector.schema.evolution.iceberg.renderers import render_sql

    return render_sql(delta, "starrocks", options)
