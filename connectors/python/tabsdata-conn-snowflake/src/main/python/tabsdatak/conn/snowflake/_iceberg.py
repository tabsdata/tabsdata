#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake-specific Iceberg schema-evolution renderer + helpers.

The dialect-agnostic infrastructure (models, delta core,
:class:`DialectRenderer` abstract base) lives in
``tabsdatak.conn.common.sql._iceberg``; this module ships only the
Snowflake-specific pieces consumed by ``_impl.py``.

Snowflake evolves a target in two directions, both of them widening: it
gains the columns an incoming file has and the table does not, and a
column both have takes the wider type. Nothing is dropped, narrowed or
reordered, so :data:`SnowflakeRenderer.SUPPORTED_ACTIONS` admits
:attr:`Action.ADD_COLUMN` and :attr:`Action.ALTER_TYPE` alone, every
other change :func:`compute_delta` reports is skipped when rendering,
and a retype the dialect cannot make safely is left to the caller to
filter on :attr:`SchemaChange.safe`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# Re-exported so callers import every iceberg piece from one module.
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._iceberg import (  # noqa: F401
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
    SUPPORTED_ACTIONS = {Action.ADD_COLUMN, Action.ALTER_TYPE}
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
        if change.action is Action.ALTER_TYPE:
            return (
                f"ALTER TABLE {table_name} ALTER COLUMN"
                f" {quote_column(change.column)}"
                f" SET DATA TYPE {change.detail['new_type']}"
            )
        return None

    def _create_table_sql(self, table_name: str, col_defs: str) -> str:
        raise NotImplementedError(_NO_CREATE_PATH)


# --- Type widening map -----------------------------------------------

# Empty by the dialect, not by omission: Snowflake has no cross-type
# widening, so no base type is ever reachable from another. What it does
# allow is a wider parameterization of the same base -- VARCHAR length,
# NUMBER precision at a fixed scale -- which a map keyed by base cannot
# express. :func:`can_widen` is what reads the parameters and answers for
# those; a caller that wants a retype marked safe passes it to
# ``compute_delta`` as ``can_safely_widen``.
SNOWFLAKE_TYPE_WIDENING_MAP: TypeWideningMap = {}

_RE = re.compile(r"^\s*(\w+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")

_ALIAS = {
    "INT": "NUMBER",
    "INTEGER": "NUMBER",
    "BIGINT": "NUMBER",
    "SMALLINT": "NUMBER",
    "TINYINT": "NUMBER",
    "BYTEINT": "NUMBER",
    "DECIMAL": "NUMBER",
    "NUMERIC": "NUMBER",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "NVARCHAR": "VARCHAR",
    "NVARCHAR2": "VARCHAR",
    "VARCHAR2": "VARCHAR",
    "CHAR": "VARCHAR",
    "CHARACTER": "VARCHAR",
    "NCHAR": "VARCHAR",
}
_FIXED_CHAR = {"CHAR", "CHARACTER", "NCHAR"}  # default length 1, not max
_ALTERABLE = {"NUMBER": (38, 0), "VARCHAR": (16777216, 0)}


class ParseError(ValueError):
    pass


def _parse(t):
    """-> (base, precision_or_length, scale). Defaults filled in."""
    m = _RE.match(t)
    if not m:
        raise ParseError(f"cannot parse type: {t!r}")

    raw = m[1].upper()
    base = _ALIAS.get(raw, raw)
    p = int(m[2]) if m[2] else None
    s = int(m[3]) if m[3] else 0

    if base == "VARCHAR" and m[3]:
        raise ParseError(f"{raw} takes one length parameter: {t!r}")
    if base == "NUMBER" and p is not None and s > p:
        raise ParseError(f"scale exceeds precision: {t!r}")

    if p is None:
        if raw in _FIXED_CHAR:
            return base, 1, 0
        if base in _ALTERABLE:
            return (base, *_ALTERABLE[base])
        return base, None, 0
    return base, p, s


def can_widen(old: str, new: str) -> bool:
    """True if `old` -> `new` is a legal Snowflake ALTER COLUMN ... SET DATA TYPE.

    Snowflake permits only: increasing VARCHAR length, and increasing NUMBER
    precision at a FIXED scale. No cross-type changes exist.
    """
    ob, op, os_ = _parse(old)
    nb, np_, ns = _parse(new)

    if ob != nb:
        return False  # no cross-type changes in Snowflake
    if nb not in _ALTERABLE:
        return False  # DATE/TIMESTAMP/FLOAT/BOOLEAN/BINARY
    if (op, os_) == (np_, ns):
        return False  # no-op (covers synonyms)
    if nb == "NUMBER" and ns != os_:
        return False  # scale is immutable
    return np_ >= op  # length / precision may only grow


def same_type(old: str, new: str) -> bool:
    """True if the two spellings name one Snowflake type."""
    try:
        return _parse(old) == _parse(new)
    except ParseError:
        return False


def same_unalterable_base(old: str, new: str) -> bool:
    """True if both spellings name one base whose parameters ``ALTER COLUMN``
    cannot change -- ``TIMESTAMP_NTZ(9)`` against ``TIMESTAMP_NTZ(6)``.
    """
    try:
        old_base, new_base = _parse(old)[0], _parse(new)[0]
    except ParseError:
        return False
    return old_base == new_base and old_base not in _ALTERABLE


# --- Schema conversion -----------------------------------------------


def table_schema_from_probe(
    table_name: str,
    rows: Iterable[tuple[str, str]],
) -> TableSchema:
    """Build a :class:`TableSchema` for the table as it stands, from ``(name,
    type)`` pairs as ``DESC TABLE`` reports them.

    Names are upper-cased so they compare the way the load matches them --
    ``MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`` -- which keeps a target created
    with a quoted lower-case column from gaining an upper-case duplicate beside
    it.

    Types are carried verbatim, in the spelling a statement would use for that
    column. They are not the spelling ``infer_schema`` reports for the same type
    -- ``VARCHAR(16777216)`` against ``TEXT`` -- so a column both sides have can
    read as a retype. That costs nothing while ``ADD_COLUMN`` is the only action
    rendered; normalizing the two vocabularies is the prerequisite for ever
    supporting ``ALTER_TYPE``.
    """
    return TableSchema(
        table_name=table_name,
        columns=[ColumnDef(name=name.upper(), type=type_) for name, type_ in rows],
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
