#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake-specific Iceberg schema-evolution renderer + helpers.

The dialect-agnostic infrastructure (models, delta core,
:class:`DialectRenderer` abstract base) lives in
``tabsdatak.conn.common.sql._iceberg``; this module ships only the
Snowflake-specific pieces consumed by ``_impl.py``.

Snowflake evolves a target one way only: it gains the columns an
incoming file has and the table does not. Nothing is dropped, retyped or
reordered, so :data:`SnowflakeRenderer.SUPPORTED_ACTIONS` admits
:attr:`Action.ADD_COLUMN` alone and every other change
:func:`compute_delta` reports is skipped when rendering.
"""

from __future__ import annotations

from collections.abc import Iterable

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
    # Re-exported so callers import every iceberg piece from one module.
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
    # Snowflake-specific surface.
    "SNOWFLAKE_TYPE_WIDENING_MAP",
    "SnowflakeRenderer",
    "quote_column",
    "render_snowflake_sql",
    "table_schema_from_inferred",
    "table_schema_from_probe",
]


# The connector never builds a table from rendered column definitions:
# ``_impl._create_table_from_inferred_schema`` issues ``CREATE TABLE ...
# USING TEMPLATE (infer_schema(...))`` so Snowflake picks each column's
# type from the parquet itself. Rendering a second, divergent CREATE here
# would be a silent fork of that decision.
_NO_CREATE_PATH = (
    "SnowflakeRenderer renders ALTER statements only; a table is created"
    " from the staged file's inferred schema by"
    " _impl._create_table_from_inferred_schema"
)


def quote_column(name: str) -> str:
    """Reference a column in emitted SQL, double-quoted.

    Safe unconditionally because every name reaching here is already in
    the case Snowflake stores it under -- ``infer_schema(ignore_case =>
    true)`` upper-folds what it reports -- and quoting is the only way to
    spell a name that is not a legal bare identifier.
    """
    return '"' + name.replace('"', '""') + '"'


# --- Snowflake renderer ----------------------------------------------


class SnowflakeRenderer(DialectRenderer):
    SUPPORTED_ACTIONS = {Action.ADD_COLUMN}
    SUPPORTS_REORDER = False

    def column_def(
        self,
        name: str,
        type_: str,
        nullable: bool,
        default: str | None,
    ) -> str:
        raise NotImplementedError(_NO_CREATE_PATH)

    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        if change.action is Action.ADD_COLUMN:
            # No NOT NULL and no DEFAULT: the rows already in the table
            # have no value for a column being added to it.
            return (
                f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS"
                f" {quote_column(change.column)} {change.detail['type']}"
            )
        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        raise NotImplementedError(_NO_CREATE_PATH)


# --- Type widening map -----------------------------------------------

# Empty: ``ALTER_TYPE`` is not a supported action, so no current type is
# ever compared against an incoming one. Populating this map alone would
# not enable widening -- ``SnowflakeRenderer.SUPPORTED_ACTIONS`` would
# have to admit the action too, and the current schema would have to
# carry real types (see :func:`table_schema_from_probe`).
SNOWFLAKE_TYPE_WIDENING_MAP: TypeWideningMap = {}


# --- Schema conversion -----------------------------------------------


def table_schema_from_probe(table_name: str, names: Iterable[str]) -> TableSchema:
    """Build a :class:`TableSchema` for the table as it stands, from the column
    names of an empty result set.

    Names are upper-cased so they compare the way the load matches them --
    ``MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`` -- which keeps a target created
    with a quoted lower-case column from gaining an upper-case duplicate beside
    it.

    Types are left empty. Nothing reads them: the only action rendered is
    ``ADD_COLUMN``, which describes a column the current schema does not have.
    A real type read (``information_schema.columns``, normalized against what
    ``infer_schema`` reports) is the prerequisite for ever supporting
    ``ALTER_TYPE``.
    """
    return TableSchema(
        table_name=table_name,
        columns=[ColumnDef(name=name.upper(), type="") for name in names],
    )


def table_schema_from_inferred(
    table_name: str,
    rows: Iterable[tuple[str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` for the schema an incoming file wants, from
    ``(name, type)`` pairs as ``infer_schema`` reports them.

    Every column is nullable: a column added to a populated table cannot be
    ``NOT NULL``, and Iceberg semantics only count an added column safe when it
    is optional or has a default.
    """
    return TableSchema(
        table_name=table_name,
        columns=[ColumnDef(name=name, type=type_) for name, type_ in rows],
    )


def render_snowflake_sql(
    delta: SchemaDelta,
    options: RenderOptions | None = None,
) -> list[str]:
    """Render a SchemaDelta as Snowflake SQL statements."""
    return SnowflakeRenderer().render_sql(delta, options)
