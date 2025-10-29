#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Iterable, get_args

import polars as pl

# noinspection PyProtectedMember
import polars._typing as pl_typing

import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.frame as td_frame

# noinspection PyProtectedMember
from tabsdata._utils.annotations import pydoc
from tabsdata.tableframe.typing import ConcatMethod


@pydoc(categories="union")
def concat(
    items: Iterable[td_frame.TdType],
    how: ConcatMethod = "vertical",
) -> td_frame.TdType:
    """
    Combine multiple TableFrames by stacking their rows.

    Args:
        items: The TableFrames to concatenate.
        how: {'vertical', 'vertical_relaxed', 'diagonal', 'diagonal_relaxed'}
            * vertical: Appends the rows of each input below the previous one. All
              inputs must have exactly the same column names and types; otherwise the
              operation fails.
            * vertical_relaxed: Same as `vertical`, but if columns with the same name
              have different data types across inputs, they are converted to a common
              type (e.g. Int32 → Int64).
            * diagonal: Aligns columns by name across all inputs. If a column is missing
              from a particular input, that input is padded with `null` values for the
              missing column. Matching columns keep their original type if consistent.
            * diagonal_relaxed: Same as `diagonal`, but when matching columns have
              different data types, they are converted to a common type
              (e.g. Int32 → Int64).

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
    >>> tf = td.concat([tf1, tf2])
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
    valid_methods = get_args(ConcatMethod)
    if how not in valid_methods:
        raise ValueError(
            f"Invalid concatenation method: {how!r}. Expected one of {valid_methods}"
        )

    unwrapped_items = [_unwrap_td_ype(item) for item in items]

    # First, perform a no-op concatenation of empty LazyFrames built from the
    # schemas of the original frames. This ensures that all schemas are compatible.
    # Without this step, a mismatch might only surface later with a less clear error.
    schemas = [lf.collect_schema() for lf in unwrapped_items]
    empties = [
        pl.DataFrame(schema={name: schema[name] for name in schema.names()})
        for schema in schemas
    ]
    pl.concat(
        empties,
        how=how,
        rechunk=False,
        parallel=True,
    )

    polars_type = pl.concat(
        unwrapped_items,
        how=how,
        rechunk=False,
        parallel=True,
    )
    wrapped_item = _wrap_polars_type(polars_type)
    return wrapped_item


def _wrap_polars_type(
    obj: pl_typing.PolarsType,
) -> td_frame.TdType:
    if isinstance(obj, pl.LazyFrame):
        # noinspection PyProtectedMember
        return td_frame.TableFrame.__build__(
            df=obj,
            mode="tab",
            idx=None,
            properties=None,
        )
    elif isinstance(obj, pl.Expr):
        # noinspection PyProtectedMember
        return td_expr.Expr(obj)
    else:
        return obj


# noinspection PyTypeChecker
def _unwrap_td_ype(
    obj: td_frame.TdType,
) -> pl_typing.PolarsType:
    if isinstance(obj, td_frame.TableFrame):
        # noinspection PyProtectedMember
        return obj._lf
    elif isinstance(obj, td_expr.Expr):
        # noinspection PyProtectedMember
        return obj._expr
    else:
        return obj
