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
