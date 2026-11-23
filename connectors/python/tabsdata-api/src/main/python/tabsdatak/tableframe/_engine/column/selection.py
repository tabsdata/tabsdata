#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.column.registry import Registry as Registry_


def keep_system_columns(
    call: Callable[[pl_.LazyFrame], pl_.LazyFrame],
    lazyframe: pl_.LazyFrame,
    params: dict,
    registry: Registry_,
) -> tuple[pl_.LazyFrame, list[str]]:
    """Run a select, keeping the system columns it would drop when it keeps the
    rows: a select of plain columns leaves every row as it was, so each row keeps
    its own system columns (a stacked frame's rows keep their own version).
    Returns the result and the system columns it still lacks, which any other
    select may change, reorder or reduce, so they are regenerated."""
    selected = call(lazyframe)
    kept = set(selected.collect_schema().names())
    dropped = [
        name
        for name in lazyframe.collect_schema().names()
        if registry.kind_of(name) is ColumnKind.INTERNAL and name not in kept
    ]
    if not dropped:
        return selected, []
    if all(keeps_rows(expression) for expression in projection(params)):
        return lazyframe.select(*projection(params), *dropped), []
    return selected, dropped


def projection(params: dict) -> list:
    """A select's expressions: the positional ones, lists flattened, and the
    named ones aliased."""
    return [
        *_flatten(params["exprs"]),
        *(_named(name, value) for name, value in params["named"].items()),
    ]


def hide_internal_columns(selection: pl_.Expr, names: tuple) -> pl_.Expr:
    """``selection`` without the internal columns when ``names`` pick columns by
    pattern (a wildcard, a regular expression or a data type), so a frame-wide
    expression never reaches them. Columns named one by one, internal ones
    included, are kept as named."""
    import re

    from tabsdatak.tableframe._engine.constant.column.prefixes import (
        TD_SYSTEM_COLUMN_PREFIX,
    )

    if not _by_pattern(names):
        return selection
    return selection.exclude(f"^{re.escape(TD_SYSTEM_COLUMN_PREFIX)}.*$")


def hide_internal_patterns(columns: tuple) -> tuple:
    """``columns`` with every name pattern (a wildcard or a regular expression)
    turned into a selection without the internal columns, so dropping by pattern
    never drops them. Columns named one by one are kept as named."""
    import polars as pl

    narrowed: list = []
    for column in columns:
        if isinstance(column, (list, tuple)):
            narrowed += hide_internal_patterns(column)
        elif isinstance(column, str) and _by_pattern((column,)):
            narrowed.append(hide_internal_columns(pl.col(column), (column,)))
        else:
            narrowed.append(column)
    return tuple(narrowed)


def _by_pattern(names) -> bool:
    for name in names:
        if isinstance(name, (list, tuple)):
            if _by_pattern(name):
                return True
        elif not isinstance(name, str):
            return True
        elif name == "*" or (name.startswith("^") and name.endswith("$")):
            return True
    return False


def keeps_rows(expression) -> bool:
    import polars as pl

    if isinstance(expression, str):
        return True
    if isinstance(expression, pl.Expr):
        return expression.meta.is_column_selection(allow_aliasing=True)
    return False


def _flatten(values) -> list:
    flat: list = []
    for value in values:
        if isinstance(value, (list, tuple)):
            flat += list(value)
        else:
            flat.append(value)
    return flat


def _named(name: str, value) -> pl_.Expr:
    import polars as pl

    if isinstance(value, str):
        return pl.col(value).alias(name)
    if isinstance(value, pl.Expr):
        return value.alias(name)
    return pl.lit(value).alias(name)
