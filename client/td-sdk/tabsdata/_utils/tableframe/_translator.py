#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any, Collection, List, Union, get_args

import polars as pl
from typing_extensions import get_origin

import tabsdata._utils.tableframe._common as td_common
import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.functions.datetime as td_datetime
import tabsdata.tableframe.lazyframe.frame as td_frame
from tabsdata.exceptions import ErrorCode, TableFrameError
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder


def _is_instance_of_union(obj, tp):
    origin = get_origin(tp)
    if origin is Union:
        args = get_args(tp)
        return any(isinstance(obj, arg) for arg in args if isinstance(arg, type))
    return False


def _wrap_polars_frame(f: pl.LazyFrame | pl.DataFrame) -> td_frame.TableFrame:
    """Use only for testing."""
    if isinstance(f, pl.LazyFrame):
        # noinspection PyProtectedMember
        return td_frame.TableFrame.__build__(
            df=f,
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        # return td_frame.TableFrame._from_lazy(f)
    elif isinstance(f, pl.DataFrame):
        # noinspection PyProtectedMember
        return td_frame.TableFrame.__build__(
            df=f.lazy(),
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )
        # noinspection PyProtectedMember
        # return td_frame.TableFrame._from_lazy(f.lazy())
    else:
        raise TableFrameError(ErrorCode.TF7, type(f))


def _unwrap_table_frame(tf: td_frame.TableFrame) -> pl.LazyFrame:
    # noinspection PyProtectedMember
    return td_common.drop_system_columns(lf=tf._to_lazy())


# noinspection PyProtectedMember
def _unwrap_tdexpr(expr: Any) -> Union[pl.Expr | List[pl.Expr] | Any]:
    if isinstance(expr, td_expr.Expr):
        return expr._expr
    elif isinstance(expr, dict):
        return {
            key: value._expr if isinstance(value, td_expr.Expr) else value
            for key, value in expr.items()
        }
    elif isinstance(expr, Collection) and not isinstance(expr, (str, bytes)):
        return [item._expr if isinstance(item, td_expr.Expr) else item for item in expr]
    else:
        return expr


def _unwrap_into_tdexpr_column(
    expr: Any,
) -> Union[pl.IntoExprColumn | List[pl.IntoExprColumn] | Any]:
    if isinstance(expr, td_expr.Expr):
        # noinspection PyProtectedMember
        return expr._expr
    elif isinstance(expr, dict):
        return {key: _unwrap_into_tdexpr_column(value) for key, value in expr.items()}
    elif isinstance(expr, Collection) and not isinstance(expr, (str, bytes)):
        # noinspection PyProtectedMember
        return [item._expr if isinstance(item, td_expr.Expr) else item for item in expr]
    else:
        return expr


def _unwrap_into_tdexpr(expr: Any) -> Union[pl.IntoExpr | List[pl.IntoExpr] | Any]:
    if expr is None:
        return None
    if isinstance(expr, (td_expr.Expr, pl.Series, str)):
        # noinspection PyProtectedMember
        return _unwrap_into_tdexpr_column(expr)
    elif isinstance(expr, dict):
        return {key: _unwrap_into_tdexpr_column(value) for key, value in expr.items()}
    elif isinstance(expr, Collection) and not isinstance(expr, (str, bytes)):
        return [_unwrap_into_tdexpr_column(item) for item in expr]
    else:
        return expr


def _unwrap_tdexpr_date_time_name_space(
    expr: td_datetime.ExprDateTimeNameSpace,
) -> pl.ExprDateTimeNameSpace:
    if isinstance(expr, td_datetime.ExprDateTimeNameSpace):
        # noinspection PyProtectedMember
        return expr._expr
    else:
        return expr


def _args_to_tuple(*args) -> tuple:
    return tuple(
        elem
        for arg in args
        for elem in (arg if isinstance(arg, (list, tuple)) else (arg,))
    )
