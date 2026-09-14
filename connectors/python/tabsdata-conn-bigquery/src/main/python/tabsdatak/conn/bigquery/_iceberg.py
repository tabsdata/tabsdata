#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery-specific Iceberg schema-evolution renderer + helpers.

The dialect-agnostic infrastructure (models, delta core,
:class:`DialectRenderer` abstract base) lives in
``tabsdatak.conn.common.sql._iceberg``; this module ships only the
BigQuery-specific pieces consumed by ``_impl.py``.

BigQuery evolves a target one way only: it gains the columns the staging
table has and the table does not. Nothing is dropped, retyped or
reordered, so :data:`BigQueryRenderer.SUPPORTED_ACTIONS` admits
:attr:`Action.ADD_COLUMN` alone and every other change
:func:`compute_delta` reports is skipped when rendering.

The renderer covers both paths a merge needs: the ``CREATE TABLE`` for a
target that does not exist yet, and the ``ALTER TABLE`` for one that does.
Both are built from the staging table's schema, so the target a merge
creates and the target a merge widens agree on what a column is.
"""

from __future__ import annotations

from collections.abc import Iterable

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._validation import _BIGQUERY_COLUMN_FQN_DEF

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import fqn

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
    # BigQuery-specific surface.
    "BIGQUERY_TYPE_WIDENING_MAP",
    "BigQueryRenderer",
    "quote_column",
    "render_bigquery_sql",
    "table_schema_from_columns",
]


def quote_column(name: str) -> str:
    """Reference a column in emitted SQL, backtick-quoted.

    Safe unconditionally: backticks are BigQuery's escape, not a
    case-sensitivity marker, and quoting is the only way to spell a
    flexible column name or one that collides with a reserved word.
    """
    return fqn([name], _BIGQUERY_COLUMN_FQN_DEF)


# --- BigQuery renderer -----------------------------------------------


class BigQueryRenderer(DialectRenderer):
    SUPPORTED_ACTIONS = {Action.ADD_COLUMN}
    SUPPORTS_REORDER = False

    def column_def(
        self,
        name: str,
        type_: str,
        nullable: bool,
        default: str | None,
    ) -> str:
        # BigQuery takes DEFAULT before NOT NULL, the reverse of most
        # dialects. Both are spelled here because the base class hands
        # them over; the schemas this connector builds are all nullable
        # and defaultless, so a CREATE it renders carries neither.
        parts = [quote_column(name), type_]
        if default is not None:
            parts.append(f"DEFAULT {default}")
        if not nullable:
            parts.append("NOT NULL")
        return " ".join(parts)

    def render_alter(
        self,
        table_name: str,
        change: SchemaChange,
    ) -> str | None:
        if change.action is Action.ADD_COLUMN:
            # No NOT NULL and no DEFAULT: the rows already in the table
            # have no value for a column being added to it, and BigQuery
            # refuses to add a REQUIRED column to a populated table.
            return (
                f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS"
                f" {quote_column(change.column)} {change.detail['type']}"
            )
        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        # ``IF NOT EXISTS`` so the create is idempotent: the caller does not
        # have to probe for the table first, and two runs starting against
        # the same missing target cannot race each other into an error.
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_defs}\n)"


# --- Type widening map -----------------------------------------------

# Empty: ``ALTER_TYPE`` is not a supported action, so no current type is
# ever compared against an incoming one. Populating this map alone would
# not enable widening -- ``BigQueryRenderer.SUPPORTED_ACTIONS`` would
# have to admit the action too, and the current schema would have to
# carry real types (see :func:`table_schema_from_names`).
BIGQUERY_TYPE_WIDENING_MAP: TypeWideningMap = {}


# --- Schema conversion -----------------------------------------------


def table_schema_from_columns(
    table_name: str,
    columns: Iterable[tuple[str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` for the schema the incoming data wants, from
    ``(name, type)`` pairs.

    Every column is nullable: a column added to a populated table cannot be
    ``REQUIRED``, and Iceberg semantics only count an added column safe when it
    is optional or has a default.
    """
    return TableSchema(
        table_name=table_name,
        columns=[ColumnDef(name=name, type=type_) for name, type_ in columns],
    )


def render_bigquery_sql(
    delta: SchemaDelta,
    options: RenderOptions | None = None,
) -> list[str]:
    """Render a SchemaDelta as BigQuery SQL statements."""
    return BigQueryRenderer().render_sql(delta, options)
