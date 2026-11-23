#
# Copyright 2026 Tabsdata Inc.
#

"""Frame-wide ops limited to the user columns.

An op that reaches every column of a frame -- fill every null, cast every column
of a type -- is meant for the data the user wrote, not for the system and
meta columns riding along, which it would otherwise rewrite as a side
effect.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl_


def on_user_columns(
    lf: pl_.LazyFrame,
    user: list[str],
    apply: Callable[[pl_.LazyFrame], pl_.LazyFrame],
) -> pl_.LazyFrame:
    """``apply`` to the user columns alone, the others kept as they are.

    The others ride through packed in one struct column, which no frame-wide
    op reaches: it never matches a fill value's type and is never null itself.
    That keeps the plan a single pass -- splitting the frame in two and putting
    it back together would compute everything upstream twice.
    """
    import polars as pl

    from tabsdatak.tableframe._engine.column.generator import temp_column

    names = lf.collect_schema().names()
    others = [name for name in names if name not in user]
    if not user or not others:
        return apply(lf) if user else lf
    packed = temp_column()
    return (
        apply(lf.with_columns(pl.struct(others).alias(packed)).drop(others))
        .unnest(packed)
        .select(names)
    )


def user_cast(schema: pl_.Schema, user: list[str], dtypes) -> dict:
    """``dtypes`` with every by-type target narrowed to the user columns of that
    type. A target named explicitly is left as it is, unless it is an internal
    column, which a cast must not change."""
    from tabsdatak.tableframe._engine.constant.column.prefixes import (
        TD_SYSTEM_COLUMN_PREFIX,
    )

    if not isinstance(dtypes, Mapping):
        return {name: dtypes for name in user}
    internal = sorted(
        key
        for key in dtypes
        if isinstance(key, str) and key.startswith(TD_SYSTEM_COLUMN_PREFIX)
    )
    if internal:
        from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

        raise TableFrameErrorCode.TF6.exception(op="cast", columns=internal)
    mapping = {}
    for key, target in dtypes.items():
        if isinstance(key, str):
            mapping[key] = target
            continue
        for name in user:
            if schema[name] == key or (
                isinstance(key, type) and isinstance(schema[name], key)
            ):
                mapping[name] = target
    return mapping
