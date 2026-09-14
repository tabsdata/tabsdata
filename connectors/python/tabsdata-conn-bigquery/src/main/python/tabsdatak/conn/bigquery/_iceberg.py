#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery-specific Iceberg schema-evolution renderer + helpers.

The dialect-agnostic infrastructure (models, delta core,
:class:`DialectRenderer` abstract base) lives in
``tabsdatak.conn.common.sql._iceberg``; this module ships only the
BigQuery-specific pieces consumed by ``_impl.py``.

BigQuery evolves a target in two directions, both of them widening: it
gains the columns the staging table has and the table does not, and a
column both have takes the wider of the two types. Nothing is dropped,
narrowed or reordered, so :data:`BigQueryRenderer.SUPPORTED_ACTIONS`
admits :attr:`Action.ADD_COLUMN` and :attr:`Action.ALTER_TYPE` alone,
every other change :func:`compute_delta` reports is skipped when
rendering, and a retype the dialect cannot make safely is left to the
caller to filter on :attr:`SchemaChange.safe`.

The renderer covers both paths a merge needs: the ``CREATE TABLE`` for a
target that does not exist yet, and the ``ALTER TABLE`` for one that does.
Both are built from the staging table's schema, so the target a merge
creates and the target a merge widens agree on what a column is.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from math import inf

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
    SUPPORTED_ACTIONS = {Action.ADD_COLUMN, Action.ALTER_TYPE}
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
        if change.action is Action.ALTER_TYPE:
            # Whether the conversion is one BigQuery will make is
            # ``change.safe``, which the caller gates on -- a renderer
            # renders. ``IF EXISTS`` for the same reason the ADD carries
            # ``IF NOT EXISTS``: a concurrent run may have been here first.
            return (
                f"ALTER TABLE {table_name} ALTER COLUMN IF EXISTS"
                f" {quote_column(change.column)}"
                f" SET DATA TYPE {change.detail['new_type']}"
            )
        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        # ``IF NOT EXISTS`` so the create is idempotent: the caller does not
        # have to probe for the table first, and two runs starting against
        # the same missing target cannot race each other into an error.
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_defs}\n)"


# --- Type widening map -----------------------------------------------

# Which base type can become which, read by ``compute_delta`` to mark a
# retype safe. It answers for the base alone: ``_impl.column_type`` spells
# a type with its parameters, so a ``NUMERIC(20, 4)`` matches none of these
# keys and :func:`can_widen` -- passed to ``compute_delta`` alongside -- is
# what reads the parameters and has the final say.
BIGQUERY_TYPE_WIDENING_MAP: TypeWideningMap = {
    "NUMERIC": {"INT64"},
    "BIGNUMERIC": {"INT64", "NUMERIC"},
    "FLOAT64": {"INT64", "NUMERIC", "BIGNUMERIC"},
}

_RE = re.compile(r"^\s*(\w+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")
_ALIAS = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "DECIMAL": "NUMERIC",
    "BIGDECIMAL": "BIGNUMERIC",
}
_MAX = {
    "NUMERIC": (38 - 9, 9),
    "BIGNUMERIC": (76 - 38, 38),
    "INT64": (19, 0),
    "FLOAT64": (inf, inf),
    "STRING": (inf,),
    "BYTES": (inf,),
}
_LENGTH = ("STRING", "BYTES")


def _parse(t):
    m = _RE.match(t)
    if not m:
        raise ValueError(f"cannot parse type: {t!r}")
    base = _ALIAS.get(m[1].upper(), m[1].upper())
    p = int(m[2]) if m[2] else None
    s = int(m[3]) if m[3] else 0
    if base in _LENGTH and m[3]:
        raise ValueError(f"{base} takes one length parameter: {t!r}")
    return base, p, s


def _domain(base, p, s):
    """Capacity budgets that must not shrink. Compared elementwise."""
    if p is None:
        return _MAX[base]
    return (p,) if base in _LENGTH else (p - s, s)


def can_widen(old: str, new: str) -> bool:
    """True if `old` can be widened to `new` via ALTER COLUMN SET DATA TYPE."""
    ob, op, os_ = _parse(old)
    nb, np_, ns = _parse(new)

    if (ob, op, os_) == (nb, np_, ns):
        return False  # identical, no-op
    if ob != nb and ob not in BIGQUERY_TYPE_WIDENING_MAP.get(nb, ()):
        return False  # base types incompatible
    if nb not in _MAX:
        return False  # type has no widenings

    return all(n >= o for n, o in zip(_domain(nb, np_, ns), _domain(ob, op, os_)))


# --- Schema conversion -----------------------------------------------


def table_schema_from_columns(
    table_name: str,
    columns: Iterable[tuple[str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` from ``(name, type)`` pairs -- the target as
    it stands, or the schema the incoming data wants.

    Names are taken verbatim. BigQuery reports each column in the case it was
    defined with, and folding them here would make a column both sides have
    read as a rename; aligning the two sides' spellings is the caller's job,
    since only it knows which side a name came from.

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
