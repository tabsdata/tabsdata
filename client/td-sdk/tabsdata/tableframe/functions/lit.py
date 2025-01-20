#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any

import polars as pl

# noinspection PyProtectedMember
from polars._typing import PolarsDataType

import tabsdata.tableframe.expr.expr as td_expr


def lit(
    value: Any, dtype: PolarsDataType | None = None, *, allow_object: bool = False
) -> td_expr.TdExpr:
    """
    Expression for the given literal value.

    Args:
        value: The literal value.
        dtype: The data type of the literal value.
        allow_object: Whether to allow object data type.

    Example:

    >>> import tabsdata as td
    >>>
    >>> tf: td.TableFrame ...
    >>>
    >>> tf.select(td.lit("Hi").alias("lit"), td.col("age").alias("Age"))
    >>>
    ┌──────┬──────┐
    │ lit  ┆ Age  │
    │ ---  ┆ ---  │
    │ str  ┆ i64  │
    ╞══════╪══════╡
    │ Hi   ┆ 1    │
    │ Hi   ┆ 15   │
    │ Hi   ┆ 18   │
    │ Hi   ┆ null │
    └──────┴──────┘
    """
    return td_expr.TdExpr(pl.lit(value, dtype, allow_object=allow_object))
