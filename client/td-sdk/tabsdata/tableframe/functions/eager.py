#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Iterable

import polars as pl

# noinspection PyProtectedMember
from polars._typing import PolarsType

import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.frame as td_frame


def concat(
    items: Iterable[td_frame.TdPolarsType],
) -> td_frame.TdPolarsType:
    """
    Combine multiple TableFrames by stacking their rows.

    Args:
        items: The TableFrames to concatenate.

    Example:

    >>> import tabsdata as td
    >>>
    >>> tf1: td.TableFrame ...
    >>>
    ┌──────┬──────┐
    │ a    ┆ b    │
    │ ---  ┆ ---  │
    │ str  ┆ i64  │
    ╞══════╪══════╡
    │ a    ┆ 1    │
    │ b    ┆ 2    │
    └──────┴──────┘
    >>>
    >>> tf2: td.TableFrame ...
    >>>
    ┌──────┬──────┐
    │ a    ┆ b    │
    │ ---  ┆ ---  │
    │ str  ┆ i64  │
    ╞══════╪══════╡
    │ x    ┆ 10   │
    │ y    ┆ 20   │
    └──────┴──────┘
    >>>
    >>> tf = td.concat(tf1, tf2)
    >>>
    ┌──────┬──────┐
    │ a    ┆ b    │
    │ ---  ┆ ---  │
    │ str  ┆ i64  │
    ╞══════╪══════╡
    │ a    ┆ 1    │
    │ b    ┆ 2    │
    │ x    ┆ 10   │
    │ y    ┆ 20   │
    └──────┴──────┘
    """
    unwrapped_items = (_unwrap_tdpolars_type(item) for item in items)
    polars_type = pl.concat(
        unwrapped_items,
        how="vertical",
        rechunk=False,
        parallel=True,
    )
    wrapped_item = _wrap_polars_type(polars_type)
    return wrapped_item


def _wrap_polars_type(
    obj: PolarsType,
) -> td_frame.TdPolarsType:
    if isinstance(obj, pl.LazyFrame):
        # noinspection PyProtectedMember
        return td_frame.TdLazyFrame.__build__(obj)
    elif isinstance(obj, pl.Expr):
        # noinspection PyProtectedMember
        return td_expr.TdExpr(obj)
    else:
        return obj


# noinspection PyTypeChecker
def _unwrap_tdpolars_type(
    obj: td_frame.TdPolarsType,
) -> PolarsType:
    if isinstance(obj, td_frame.TdLazyFrame):
        # noinspection PyProtectedMember
        return obj._lf
    elif isinstance(obj, td_expr.TdExpr):
        # noinspection PyProtectedMember
        return obj._expr
    else:
        return obj
