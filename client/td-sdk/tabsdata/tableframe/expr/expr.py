#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import builtins
import math
from typing import (
    Any,
    Collection,
    Iterable,
    NoReturn,
    ParamSpec,
    TypeVar,
    Union,
)

import polars as pl
from accessify import accessify
from typing_extensions import deprecated

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._common as td_common

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator
import tabsdata.tableframe.expr.string as td_string
import tabsdata.tableframe.functions.datetime as td_datetime

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing

# noinspection PyProtectedMember
from tabsdata._utils.annotations import deprecation, pydoc
from tabsdata.exceptions import ErrorCode, TableFrameError

T = TypeVar("T")
P = ParamSpec("P")


@accessify
class Expr:
    def __init__(self, expr: Union[pl.Expr | Expr]) -> None:
        if isinstance(expr, pl.Expr):
            self._expr = expr
        elif isinstance(expr, Expr):
            self._expr = expr._expr
        else:
            raise TableFrameError(ErrorCode.TF5, type(expr))

    """ Dunder Operations """

    def __repr__(self) -> builtins.str:
        return self._expr.__repr__()

    def __str__(self) -> builtins.str:
        return self._expr.__str__()

    def __bool__(self) -> NoReturn:
        return self._expr.__bool__()

    def __abs__(self) -> Expr:
        return Expr(self._expr.__abs__())

    def __add__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__add__(td_translator._unwrap_into_tdexpr(other)))

    def __radd__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__add__(td_translator._unwrap_into_tdexpr(other)))

    def __and__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__and__(td_translator._unwrap_into_tdexpr(other)))

    def __rand__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__and__(td_translator._unwrap_into_tdexpr(other)))

    def __eq__(self, other: td_typing.IntoExpr) -> Expr:  # type: ignore[override]
        # noinspection PyProtectedMember
        return Expr(self._expr.__eq__(td_translator._unwrap_into_tdexpr(other)))

    def __floordiv__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__floordiv__(td_translator._unwrap_into_tdexpr(other)))

    def __rfloordiv__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__rfloordiv__(td_translator._unwrap_into_tdexpr(other)))

    def __ge__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__ge__(td_translator._unwrap_into_tdexpr(other)))

    def __gt__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__gt__(td_translator._unwrap_into_tdexpr(other)))

    def __invert__(self) -> Expr:
        return Expr(self._expr.__invert__())

    def __le__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__le__(td_translator._unwrap_into_tdexpr(other)))

    def __lt__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__lt__(td_translator._unwrap_into_tdexpr(other)))

    def __mod__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__mod__(td_translator._unwrap_into_tdexpr(other)))

    def __rmod__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__mod__(td_translator._unwrap_into_tdexpr(other)))

    def __mul__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__mul__(td_translator._unwrap_into_tdexpr(other)))

    def __rmul__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__mul__(td_translator._unwrap_into_tdexpr(other)))

    def __ne__(self, other: td_typing.IntoExpr) -> Expr:  # type: ignore[override]
        # noinspection PyProtectedMember
        return Expr(self._expr.__ne__(td_translator._unwrap_into_tdexpr(other)))

    def __neg__(self) -> Expr:
        return Expr(-self._expr)

    def __or__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__or__(td_translator._unwrap_into_tdexpr(other)))

    def __ror__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__or__(td_translator._unwrap_into_tdexpr(other)))

    def __pos__(self) -> Expr:
        return Expr(self._expr + self._expr)

    def __pow__(self, exponent: td_typing.IntoExprColumn | int | float) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr ** td_translator._unwrap_into_tdexpr(exponent))

    def __rpow__(self, base: td_typing.IntoExprColumn | int | float) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr ** td_translator._unwrap_into_tdexpr(base))

    def __sub__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__sub__(td_translator._unwrap_into_tdexpr(other)))

    def __rsub__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__sub__(td_translator._unwrap_into_tdexpr(other)))

    def __truediv__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__truediv__(td_translator._unwrap_into_tdexpr(other)))

    def __rtruediv__(self, other: td_typing.IntoExpr) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__truediv__(td_translator._unwrap_into_tdexpr(other)))

    def __xor__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__xor__(td_translator._unwrap_into_tdexpr(other)))

    def __rxor__(self, other: td_typing.IntoExprColumn | int | bool) -> Expr:
        # noinspection PyProtectedMember
        return Expr(self._expr.__xor__(td_translator._unwrap_into_tdexpr(other)))

    def __getstate__(self) -> bytes:
        return self._expr.__getstate__()

    def __setstate__(self, state: bytes) -> None:
        self._expr.__setstate__(state)

    """ Object Operations """

    @pydoc(categories="numeric")
    def abs(self) -> Expr:
        """
        Return the abso lute value of the expression.
        """
        return Expr(self._expr.abs())

    @pydoc(categories="numeric")
    def add(self, other: Any) -> Expr:
        """
        Equivalent to the `+` operator.

        For numeric types adds the given input. For string types concatenates the
        given input.

        Args:
            other: The value to add or concatenate.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").add(1).alias("add"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ add      │
        │ ---  ┆ ---      │
        │ i64  ┆ i64      │
        ╞══════╪══════════╡
        │ 1    ┆ 2        │
        │ 15   ┆ 16       │
        │ 18   ┆ 19       │
        │ 60   ┆ 61       │
        │ 60   ┆ 61       │
        │ 75   ┆ 76       │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.add(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="manipulation")
    def alias(self, name: builtins.str) -> Expr:
        """
        Set the name for a column or expression.

        Args:
            name: Column or expression new name. The name must be a word
                  ([A\\-Za\\-z\\_][A\\-Za\\-z0\\-9\\_]*) of up to 100 characters.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("age"), td.col("age").alias("Age"))
        >>>
        ┌──────┬──────┐
        │ age  ┆ Age  │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 1    │
        │ 15   ┆ 15   │
        │ 18   ┆ 18   │
        │ 60   ┆ 60   │
        │ 60   ┆ 60   │
        │ 75   ┆ 75   │
        │ null ┆ null │
        └──────┴──────┘
        """
        # TODO: check name matches the regex in pydoc
        td_common.check_column(name)
        return Expr(self._expr.alias(name))

    @pydoc(categories="logic")
    def and_(self, *others: Any) -> Expr:
        """
        Bitwise `and` operator with the given expressions.
        It can be used with integer and bool types.

        Args:
            others: expressions to peform the bitwise `and` with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("i"), td.col("i").and_(2).alias("and_"))
        >>>
        ┌──────┬──────┐
        │ i    ┆ and_ │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ -1   ┆ 2    │
        │ 2    ┆ 2    │
        │ -3   ┆ 0    │
        │ 0    ┆ 0    │
        │ 5    ┆ 0    │
        │ 7    ┆ 2    │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.and_(td_translator._unwrap_into_tdexpr(*others)))

    @pydoc(categories="numeric")
    def arccos(self) -> Expr:
        """
        Calculate the inverse cosine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").arccos().alias("arccos"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arccos   │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.01 ┆ 1.560796 │
        │ 0.15 ┆ 1.420228 │
        │ 0.18 ┆ 1.38981  │
        │ 0.6  ┆ 0.927295 │
        │ 0.6  ┆ 0.927295 │
        │ 0.75 ┆ 0.722734 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arccos())

    @pydoc(categories="numeric")
    def arccosh(self) -> Expr:
        """
        Calculate the inverse hyperbolic cosine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").arccosh().alias("arccosh"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arccosh  │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.1  ┆ NaN      │
        │ 1.5  ┆ 0.962424 │
        │ 1.8  ┆ 1.192911 │
        │ 6.0  ┆ 2.477889 │
        │ 6.0  ┆ 2.477889 │
        │ 7.5  ┆ 2.703576 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arccosh())

    @pydoc(categories="numeric")
    def arcsin(self) -> Expr:
        """
        Calculate the inverse sine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").arcsin().alias("arcsin"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arcsin   │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.01 ┆ 0.01     │
        │ 0.15 ┆ 0.150568 │
        │ 0.18 ┆ 0.180986 │
        │ 0.6  ┆ 0.643501 │
        │ 0.6  ┆ 0.643501 │
        │ 0.75 ┆ 0.848062 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arcsin())

    @pydoc(categories="numeric")
    def arcsinh(self) -> Expr:
        """
        Calculate the inverse hyperbolic sine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").arcsinh().alias("arcsinh"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arcsinh  │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.1  ┆ 0.099834 │
        │ 1.5  ┆ 1.194763 │
        │ 1.8  ┆ 1.350441 │
        │ 6.0  ┆ 2.49178  │
        │ 6.0  ┆ 2.49178  │
        │ 7.5  ┆ 2.712465 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arcsinh())

    @pydoc(categories="numeric")
    def arctan(self) -> Expr:
        """
        Calculate the inverse tangent of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), , td.col("val").arctan().alias("arctan"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arctan   │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.01 ┆ 0.01     │
        │ 0.15 ┆ 0.14889  │
        │ 0.18 ┆ 0.178093 │
        │ 0.6  ┆ 0.54042  │
        │ 0.6  ┆ 0.54042  │
        │ 0.75 ┆ 0.643501 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arctan())

    @pydoc(categories="numeric")
    def arctanh(self) -> Expr:
        """
        Calculate the inverse hyperbolic tangent of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), , td.col("val").arctanh().alias("arctanh"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ arctanh  │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 0.01 ┆ 0.01     │
        │ 0.15 ┆ 0.15114  │
        │ 0.18 ┆ 0.181983 │
        │ 0.6  ┆ 0.693147 │
        │ 0.6  ┆ 0.693147 │
        │ 0.75 ┆ 0.972955 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.arctanh())

    @pydoc(categories="type_casting")
    def cast(
        self,
        dtype: td_typing.DataType | type[Any],
        *,
        strict: bool = True,
        wrap_numerical: bool = False,
    ) -> Expr:
        # noinspection PyShadowingNames
        """
        Cast a value to d different type.

        Args:
            dtype: The data type to cast to.
            strict: If false, invalid casts produce null's;
                if true, an excetion is raised.
            wrap_numerical: If true, overflowing numbers ara handled;
                if false, an excetion is raised.

        Example:

        >>> import tabsdata as td
        >>> import polars as pl
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").cast(td.Float64)).alias("cast")
        >>>
        ┌──────┬──────┐
        │ val  ┆ cast │
        │ ---  ┆ ---  │
        │ i64  ┆ f64  │
        ╞══════╪══════╡
        │ 1    ┆ 1.0  │
        │ 15   ┆ 15.0 │
        │ 18   ┆ 18.0 │
        │ 60   ┆ 60.0 │
        │ 60   ┆ 60.0 │
        │ 75   ┆ 75.0 │
        │ null ┆ null │
        └──────┴──────┘
        """
        return Expr(
            self._expr.cast(dtype, strict=strict, wrap_numerical=wrap_numerical)
        )

    @pydoc(categories="numeric")
    def cbrt(self) -> Expr:
        """
        Calculate the cub root of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").cbrt().alias("cbrt"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ cbrt     │
        │ ---  ┆ ---      │
        │ i64  ┆ f64      │
        ╞══════╪══════════╡
        │ 1    ┆ 1.0      │
        │ 15   ┆ 2.466212 │
        │ 18   ┆ 2.620741 │
        │ 60   ┆ 3.914868 │
        │ 60   ┆ 3.914868 │
        │ 75   ┆ 4.217163 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.cbrt())

    @pydoc(categories="numeric")
    def ceil(self) -> Expr:
        """
        Round up the expression to the next integer value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").first().ceil().alias("ceil"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ ceil    │
        │ ---  ┆ ------- │
        │ f64  ┆ f64     │
        ╞══════╪═════════╡
        │ 1.0  ┆  1.0    │
        │ 1.1  ┆  2.0    │
        └──────┴─────────┘
        """
        return Expr(self._expr.ceil())

    @pydoc(categories="numeric")
    def clip(
        self,
        lower_bound: (
            td_typing.NumericLiteral
            | td_typing.TemporalLiteral
            | td_typing.IntoExprColumn
            | None
        ) = None,
        upper_bound: (
            td_typing.NumericLiteral
            | td_typing.TemporalLiteral
            | td_typing.IntoExprColumn
            | None
        ) = None,
    ) -> Expr:
        """
        For element values outside the lower and upper bounds, lower values are
        replaced with the lower bound
        and upper values with the upper bound. Values within the lower and upper
        bounds are unaffected.

        Args:
            lower_bound: The lower bound value. If None, the lower bound is not set.
            upper_bound: The upper bound value. If None, the upper bound is

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("age"), td.col("age").clip(18,65).alias("clip"))
        >>>
        ┌──────┬─────────┐
        │ age  ┆ clip    │
        │ ---  ┆ ------- │
        │ i64  ┆ i64     │
        ╞══════╪═══+++═══╡
        │ 1    ┆ 18      │
        │ 18   ┆ 18      │
        │ 50   ┆ 50      │
        │ 65   ┆ 65      │
        │ 70   ┆ 65      │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(
            self._expr.clip(
                td_translator._unwrap_into_tdexpr_column(lower_bound),
                td_translator._unwrap_into_tdexpr_column(upper_bound),
            )
        )

    @pydoc(categories="numeric")
    def cos(self) -> Expr:
        """
        Calculate the cosine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").cos().alias("cos"))
        >>>
        ┌──────┬───────────┐
        │ val  ┆ cos       │
        │ ---  ┆ ---       │
        │ f64  ┆ f64       │
        ╞══════╪═══════════╡
        │ 0.1  ┆ 0.995004  │
        │ 1.5  ┆ 0.070737  │
        │ 1.8  ┆ -0.227202 │
        │ 6.0  ┆ 0.96017   │
        │ 6.0  ┆ 0.96017   │
        │ 7.5  ┆ 0.346635  │
        │ null ┆ null      │
        └──────┴───────────┘
        """
        return Expr(self._expr.cos())

    @pydoc(categories="numeric")
    def cosh(self) -> Expr:
        """
        Calculate the hyperbolic cosine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").cosh().alias("cosh"))
        >>>
        ┌──────┬────────────┐
        │ val  ┆ cosh       │
        │ ---  ┆ ---        │
        │ f64  ┆ f64        │
        ╞══════╪════════════╡
        │ 0.1  ┆ 1.005004   │
        │ 1.5  ┆ 2.35241    │
        │ 1.8  ┆ 3.107473   │
        │ 6.0  ┆ 201.715636 │
        │ 6.0  ┆ 201.715636 │
        │ 7.5  ┆ 904.021484 │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        return Expr(self._expr.cosh())

    @pydoc(categories="numeric")
    def cot(self) -> Expr:
        """
        Calculate the cotangent of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").cot().alias("cot"))
        >>>
        ┌──────┬───────────┐
        │ val  ┆ cot       │
        │ ---  ┆ ---       │
        │ f64  ┆ f64       │
        ╞══════╪═══════════╡
        │ 0.1  ┆ 9.966644  │
        │ 1.5  ┆ 0.070915  │
        │ 1.8  ┆ -0.233304 │
        │ 6.0  ┆ -3.436353 │
        │ 6.0  ┆ -3.436353 │
        │ 7.5  ┆ 0.369547  │
        │ null ┆ null      │
        └──────┴───────────┘
        """
        return Expr(self._expr.cot())

    @pydoc(categories="numeric")
    def degrees(self) -> Expr:
        """
        Convert a radian value to degrees

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").degrees().alias("degrees"))
        >>>
        ┌──────┬────────────┐
        │ val  ┆ degress    │
        │ ---  ┆ ---        │
        │ f64  ┆ f64        │
        ╞══════╪════════════╡
        │ 0.1  ┆ 5.729578   │
        │ 1.5  ┆ 85.943669  │
        │ 1.8  ┆ 103.132403 │
        │ 6.0  ┆ 343.774677 │
        │ 6.0  ┆ 343.774677 │
        │ 7.5  ┆ 429.718346 │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        return Expr(self._expr.degrees())

    @pydoc(categories="logic")
    def eq(self, other: Any) -> Expr:
        """
        Compare if 2 expressions are equal, equivalent to `expr == other`. If one
        of the expressions is `null` (None) it returns `null`.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("a").eq(td.col("b")).alias("eq"))
        >>>
        ┌─────┬──────┬───────┐
        │ a   ┆ b    ┆ eq    │
        │ --- ┆ ---  ┆ ---   │
        │ f64 ┆ f64  ┆ bool  │
        ╞═════╪══════╪═══════╡
        │ 1.0 ┆ 2.0  ┆ false │
        │ 2.0 ┆ 2.0  ┆ true  │
        │ NaN ┆ NaN  ┆ true  │
        │ 4.0 ┆ NaN  ┆ false │
        │ 5.0 ┆ null ┆ null  │
        │ null┆ null ┆ null  │
        └─────┴──────┴───────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.eq(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def eq_missing(self, other: Any) -> Expr:
        """
        Compare if 2 expressions are equal an, equivalent to `expr == other`. If one
        of the expressions is `null` (None) it returns `false`.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("a").eq(td.col("b")).alias("eq_missing"))
        >>>
        ┌─────┬──────┬───────────┐
        │ a   ┆ b    ┆ eq_missing│
        │ --- ┆ ---  ┆ ---       │
        │ f64 ┆ f64  ┆ bool      │
        ╞═════╪══════╪═══════════╡
        │ 1.0 ┆ 2.0  ┆ false     │
        │ 2.0 ┆ 2.0  ┆ true      │
        │ NaN ┆ NaN  ┆ true      │
        │ 4.0 ┆ NaN  ┆ false     │
        │ 5.0 ┆ null ┆ false     │
        │ null┆ null ┆ true      │
        └─────┴──────┴───────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.eq_missing(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def exp(self) -> Expr:
        """
        Calculate the exponential of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").exp().alias("exp"))
        >>>
        ┌──────┬─────────────┐
        │ val  ┆ exp         │
        │ ---  ┆ ---         │
        │ f64  ┆ f64         │
        ╞══════╪═════════════╡
        │ 0.1  ┆ 1.105171    │
        │ 6.0  ┆ 403.428793  │
        │ 6.0  ┆ 403.428793  │
        │ 7.5  ┆ 1808.042414 │
        │ null ┆ null        │
        └──────┴─────────────┘
        """
        return Expr(self._expr.exp())

    @pydoc(categories="manipulation")
    def fill_nan(self, value: int | float | Expr | None) -> Expr:
        """
        Replace `NaN` values with the given value.

        Args:
            value: The value to replace `NaN` values with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").fill_nan(5.5)
        >>>        .alias("fill_nan"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ fill_nan │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 1.1  ┆ 1.1      │
        │ 2.0  ┆ 2.0      │
        │ inf  ┆ inf      │
        │ null ┆ null     │
        │ NaN  ┆ 5.5      │
        └──────┴──────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.fill_nan(td_translator._unwrap_tdexpr(value)))

    @pydoc(categories="manipulation")
    def fill_null(
        self,
        value: Any | Expr | None = None,
        strategy: td_typing.FillNullStrategy | None = None,
        limit: int | None = None,
    ) -> Expr:
        """
        Replace `null` values with the given value.

        Args:
            value: The value to replace `null` values with.
            strategy: The strategy to use for filling `null` values.
            limit: The maximum number of `null` values to replace.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val")
        >>>        .fill_null(5.5).alias("fill_null"))
        >>>
        ┌──────┬───────────┐
        │ val  ┆ fill_null │
        │ ---  ┆ ---       │
        │ f64  ┆ f64       │
        ╞══════╪═══════════╡
        │ -1.0 ┆ -1.0      │
        │ 0.0  ┆ 0.0       │
        │ 1.1  ┆ 1.1       │
        │ 2.0  ┆ 2.0       │
        │ inf  ┆ inf       │
        │ null ┆ 5.5       │
        │ NaN  ┆ NaN       │
        └──────┴───────────┘
        """
        # noinspection PyProtectedMember
        return Expr(
            self._expr.fill_null(
                td_translator._unwrap_into_tdexpr(value), strategy, limit
            )
        )

    @pydoc(categories="filters")
    def filter(
        self,
        *predicates: td_typing.IntoExprColumn | Iterable[td_typing.IntoExprColumn],
    ) -> Expr:
        """
        Apply a filter predicate to an expression.

        Elements for which the predicate does not evaluate to `true` are discarded,
        evaluations to `null` are also discarded.

        Useful in an aggregation expression.

        Args:
            predicates: Expression(s) that evaluates to a boolean.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌───────┬─────────┐
        │ state ┆ tickets │
        │ ---   ┆ ---     │
        │ str   ┆ i64     │
        ╞═══════╪═════════╡
        │ CA    ┆ 1       │
        │ AL    ┆ 3       │
        │ CA    ┆ 2       │
        │ NY    ┆ 2       │
        │ NY    ┆ 3       │
        └───────┴─────────┘
        >>>
        >>> import tabsdata as td
        >>> tf.group_by("state").agg(td.col("tickets")
        >>>   .filter(td.col("tickets") !=2)
        >>>   .sum().alias("sum_non_two"))
        >>>
        ┌───────┬─────────────┐
        │ state ┆ sum_non_two │
        │ ---   ┆ ---         │
        │ str   ┆ i64         │
        ╞═══════╪═════════════╡
        │ AL    ┆ 3           │
        │ NY    ┆ 3           │
        │ CA    ┆ 1           │
        └───────┴─────────────┘
        """
        # noinspection PyProtectedMember
        return Expr(
            self._expr.filter(td_translator._unwrap_into_tdexpr_column(*predicates))
        )

    @pydoc(categories="aggregation")
    def first(self) -> Expr:
        """
        Get the first element.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("age"), td.col("age").first().alias("first"))
        >>>
        ┌──────┬─────────┐
        │ age  ┆ first   │
        │ ---  ┆ ------- │
        │ i64  ┆ i64     │
        ╞══════╪═════════╡
        │ 10   ┆ 10      │
        │ 11   ┆ 10      │
        │ 18   ┆ 10      │
        │ 65   ┆ 10      │
        │ 70   ┆ 10      │
        └──────┴─────────┘
        """
        return Expr(self._expr.first())

    @pydoc(categories="numeric")
    def floor(self) -> Expr:
        """
        Round down the expression to the previous integer value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").ceil().alias("floor"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ floor   │
        │ ---  ┆ ------- │
        │ f64  ┆ i64     │
        ╞══════╪═════════╡
        │ 1.0  ┆  1      │
        │ 1.1  ┆  1      │
        └──────┴─────────┘
        """
        return Expr(self._expr.floor())

    def floordiv(self, other: Any) -> Expr:
        """
        Calculate the floor on the division.

        Args:
            other: The value to divide by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").floordiv(2)
        >>>        .alias("floordiv"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ floordiv │
        │ ---  ┆ -------  │
        │ f64  ┆ i64      │
        ╞══════╪══════════╡
        │ 2.5  ┆  1       │
        │ 1.4  ┆  0       │
        └──────┴──────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.floordiv(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def ge(self, other: Any) -> Expr:
        """
        Greater or equal operator.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").ge(1.0).alias("ge"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ ge      │
        │ ---  ┆ ------- │
        │ f64  ┆ bool    │
        ╞══════╪═════════╡
        │ 0.9  ┆  false  │
        │ 1.1  ┆  true   │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.ge(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def gt(self, other: Any) -> Expr:
        """
        Greater than operator.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").gt(1.0).alias("gt"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ ge      │
        │ ---  ┆ ------- │
        │ f64  ┆ bool    │
        ╞══════╪═════════╡
        │ 1.0  ┆  false  │
        │ 1.1  ┆  true   │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.gt(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def hash(
        self,
        seed: int = 0,
        seed_1: int | None = None,
        seed_2: int | None = None,
        seed_3: int | None = None,
    ) -> Expr:
        """
        Compute the hash of an element value.

        Args:
            seed: The seed for the hash function.
            seed_1: The first seed for the hash function.
            seed_2: The second seed for the hash function.
            seed_3: The third seed for the hash function.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").hash(5).alias("hash"))
        >>>
        ┌──────┬─────────────────────┐
        │ temp ┆ hash                │
        │ ---  ┆ ---                 │
        │ f64  ┆ u64                 │
        ╞══════╪═════════════════════╡
        │ 1.1  ┆ 1438840365631752616 │
        │ 2.0  ┆ 4738789230185236462 │
        └──────┴─────────────────────┘
        """
        return Expr(self._expr.hash(seed, seed_1, seed_2, seed_3))

    @pydoc(categories="logic")
    def is_between(
        self,
        lower_bound: td_typing.IntoExpr,
        upper_bound: td_typing.IntoExpr,
        closed: td_typing.ClosedInterval = "both",
    ) -> Expr:
        """
        If an expression is between the given bounds.

        Args:
            lower_bound: The lower bound value.
            upper_bound: The upper bound value.
            closed: The interval type, either "both", "left", "right", or "neither"

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp")
        >>> .is_between(0, 1).alias("between"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ between │
        │ ---  ┆ ------- │
        │ f64  ┆ bool    │
        ╞══════╪═════════╡
        │-1.0  ┆  false  │
        │ 0.0  ┆  true   │
        │ 0.5  ┆  true   │
        │ 1.0  ┆  true   │
        │ 1.1  ┆  false  │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(
            self._expr.is_between(
                td_translator._unwrap_into_tdexpr(lower_bound),
                td_translator._unwrap_into_tdexpr(upper_bound),
                closed,
            )
        )

    @pydoc(categories="logic")
    def is_finite(self) -> Expr:
        """
        If an element value is finite.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_finite()
        >>>        .alias("finite"))
        >>>
        ┌──────┬────────┐
        │ temp ┆ finite │
        │ ---  ┆ ---    │
        │ f64  ┆ bool   │
        ╞══════╪════════╡
        │ 1.1  ┆ true   │
        │ 2.0  ┆ true   │
        │ inf  ┆ false  │
        └──────┴────────┘
        """
        return Expr(self._expr.is_finite())

    @pydoc(categories="logic")
    def is_in(self, other: Union[Expr | Collection[Any]]) -> Expr:
        """
        If an element value is in the given collection.

        Args:
            other: The collection to check if the element value is in.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_in([1.1, 2.2])
        >>>        .alias("is_in"))
        >>>
        ┌──────┬────────┐
        │ temp ┆ is_in  │
        │ ---  ┆ ---    │
        │ f64  ┆ bool   │
        ╞══════╪════════╡
        │ 1.1  ┆ true   │
        │ 2.0  ┆ false  │
        │ inf  ┆ false  │
        └──────┴────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.is_in(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def is_infinite(self) -> Expr:
        """
        If an element value is infinite.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp")
        >>>        .is_infinite().alias("infinite"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ infinite │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ false    │
        │ 2.0  ┆ false    │
        │ inf  ┆ true     │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_infinite())

    @pydoc(categories="logic")
    def is_nan(self) -> Expr:
        """
        If an element value is `NaN`.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_nan().alias("nan"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ nan      │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ false    │
        │ Nan  ┆ true     │
        │ None ┆ false    │
        │ inf  ┆ false    │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_nan())

    @pydoc(categories="logic")
    def is_not_nan(self) -> Expr:
        """
        If an element value is not `NaN`.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_not_nan()
        >>>        .alias("not_nan"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ not_nan  │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ true     │
        │ Nan  ┆ false    │
        │ None ┆ false    │
        │ inf  ┆ true     │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_not_nan())

    @pydoc(categories="logic")
    def is_not_null(self) -> Expr:
        """
        If an element value is not `null` (None).

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_not_null()
        >>>        .alias("null"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ not_nulL │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ true     │
        │ Nan  ┆ true     │
        │ None ┆ false    │
        │ inf  ┆ true     │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_not_null())

    @pydoc(categories="logic")
    def is_null(self) -> Expr:
        """
        If an element value is `null` (None).

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_not_null()
        >>>        .alias("null"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ null     │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ false    │
        │ Nan  ┆ false    │
        │ None ┆ true     │
        │ inf  ┆ false    │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_null())

    @pydoc(categories="logic")
    def is_unique(self) -> Expr:
        """
        If an element value is unique for all values in the column.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").is_unique()
        >>>        .alias("unique"))
        >>>
        ┌──────┬──────────┐
        │ temp ┆ unique   │
        │ ---  ┆ ---      │
        │ f64  ┆ bool     │
        ╞══════╪══════════╡
        │ 1.1  ┆ false    │
        │ 1.1  ┆ false    │
        │ None ┆ true     │
        │ 2.0  ┆ true     │
        └──────┴──────────┘
        """
        return Expr(self._expr.is_unique())

    @pydoc(categories="aggregation")
    def last(self) -> Expr:
        """
        Get the last element.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("age"), td.col("age").first().alias("last"))
        >>>
        ┌──────┬─────────┐
        │ age  ┆ last    │
        │ ---  ┆ ------- │
        │ i64  ┆ i64     │
        ╞══════╪═════════╡
        │ 10   ┆ 70      │
        │ 11   ┆ 70      │
        │ 18   ┆ 70      │
        │ 65   ┆ 70      │
        │ 70   ┆ 70      │
        └──────┴─────────┘
        """
        return Expr(self._expr.last())

    @pydoc(categories="logic")
    def le(self, other: Any) -> Expr:
        """
        Less or equal operator.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("temp"), td.col("temp").le(1.0).alias("le"))
        >>>
        ┌──────┬─────────┐
        │ temp ┆ le      │
        │ ---  ┆ ------- │
        │ f64  ┆ bool    │
        ╞══════╪═════════╡
        │ 0.9  ┆  true   │
        │ 1.1  ┆  false  │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.le(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def log(self, base: float = math.e) -> Expr:
        """
        Calculate the logarithm to the given base.

        Args:
            base: logarithm base, defaults to `e`.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").log().alias("log")
        >>>
        ┌──────┬──────────┐
        │ val  ┆ log      │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ -1.0 ┆ NaN      │
        │ 0.0  ┆ -inf     │
        │ 1.1  ┆ 0.09531  │
        │ 2.0  ┆ 0.693147 │
        │ inf  ┆ inf      │
        │ null ┆ null     │
        │ NaN  ┆ NaN      │
        └──────┴──────────┘
        """
        return Expr(self._expr.log(base))

    @pydoc(categories="numeric")
    def log1p(self) -> Expr:
        """
        Calculate the natural logarithm plus one.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").log1p().alias("log1p")
        >>>
        ┌──────┬──────────┐
        │ val  ┆ log1p    │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ -1.0 ┆ -inf     │
        │ 0.0  ┆ 0.0      │
        │ 1.1  ┆ 0.741937 │
        │ 2.0  ┆ 1.098612 │
        │ inf  ┆ inf      │
        │ null ┆ null     │
        │ NaN  ┆ NaN      │
        └──────┴──────────┘
        """
        return Expr(self._expr.log1p())

    @pydoc(categories="numeric")
    def log10(self) -> Expr:
        """
        Calculate the logarithm base 10.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").log10().alias("log10")
        >>>
        ┌──────┬──────────┐
        │ val  ┆ log10    │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ -1.0 ┆ NaN      │
        │ 0.0  ┆ -inf     │
        │ 1.1  ┆ 0.041393 │
        │ 2.0  ┆ 0.30103  │
        │ inf  ┆ inf      │
        │ null ┆ null     │
        │ NaN  ┆ NaN      │
        └──────┴──────────┘
        """
        return Expr(self._expr.log10())

    @pydoc(categories="logic")
    def lt(self, other: Any) -> Expr:
        """
        Less than operator.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").lt(1.0).alias("lt"))
        >>>
        ┌──────┬─────────┐
        │ val  ┆ tl      │
        │ ---  ┆ ------- │
        │ f64  ┆ bool    │
        ╞══════╪═════════╡
        │ 1.0  ┆  false  │
        │ 0.1  ┆  true   │
        └──────┴─────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.lt(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def mod(self, other: Any) -> Expr:
        """
        Modulus operator.

        Args:
            other: The value to divide by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").mod(5).alias("mod"))
        >>>
        ┌──────┬──────┐
        │ val  ┆ mod  │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 1    │
        │ 15   ┆ 0    │
        │ 18   ┆ 3    │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.mod(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def mul(self, other: Any) -> Expr:
        """
        Multiplication operator.

        Args:
            other: The value to multiply by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").mul(10).alias("mul"))
        >>>
        ┌──────┬──────┐
        │ val  ┆ mul  │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 10   │
        │ 15   ┆ 150  │
        │ 18   ┆ 180  │
        │ 75   ┆ 750  │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.mul(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def ne(self, other: Any) -> Expr:
        """
        Compare if 2 expressions are not equal, equivalent to `expr != other`. If one
        of the expressions is `null` (None) it returns `null`.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("a").ne(td.col("b")).alias("ne"))
        >>>
        ┌─────┬──────┬───────┐
        │ a   ┆ b    ┆ ne    │
        │ --- ┆ ---  ┆ ---   │
        │ f64 ┆ f64  ┆ bool  │
        ╞═════╪══════╪═══════╡
        │ 1.0 ┆ 2.0  ┆ true  │
        │ 2.0 ┆ 2.0  ┆ false │
        │ NaN ┆ NaN  ┆ false │
        │ 4.0 ┆ NaN  ┆ true  │
        │ 5.0 ┆ null ┆ null  │
        │ null┆ null ┆ null  │
        └─────┴──────┴───────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.ne(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def ne_missing(self, other: Any) -> Expr:
        """
        Compare if 2 expressions are not equal an, equivalent to `expr != other`. If one
        of the expressions is `null` (None) it returns `false`.

        Args:
            other: The value to compare with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("a").eq(td.col("b")).alias("ne_missing"))
        >>>
        ┌─────┬──────┬───────────┐
        │ a   ┆ b    ┆ ne_missing│
        │ --- ┆ ---  ┆ ---       │
        │ f64 ┆ f64  ┆ bool      │
        ╞═════╪══════╪═══════════╡
        │ 1.0 ┆ 2.0  ┆ true      │
        │ 2.0 ┆ 2.0  ┆ false     │
        │ NaN ┆ NaN  ┆ false     │
        │ 4.0 ┆ NaN  ┆ true      │
        │ 5.0 ┆ null ┆ true      │
        │ null┆ null ┆ false     │
        └─────┴──────┴───────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.ne_missing(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="numeric")
    def neg(self) -> Expr:
        """
        Unary minus operator.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").neg().alias("neg"))
        >>>
        ┌──────┬──────┐
        │ val  ┆ neg  │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ -1.0 ┆ 1.0  │
        │ 0.0  ┆ -0.0 │
        │ 1.1  ┆ -1.1 │
        │ 2.0  ┆ -2.0 │
        │ inf  ┆ -inf │
        │ null ┆ null │
        │ NaN  ┆ NaN  │
        └──────┴──────┘
        """
        return Expr(self._expr.neg())

    @pydoc(categories="logic")
    def not_(self) -> Expr:
        """
        Negate a boolean expression.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").not_().alias("not"))
        >>>
        ┌───────┬───────┐
        │ val   ┆ not   │
        │ ---   ┆ ---   │
        │ bool  ┆ bool  │
        ╞═══════╪═══════╡
        │ true  ┆ false │
        │ false ┆ true  │
        │ null  ┆ null  │
        └───────┴───────┘
        """
        return Expr(self._expr.not_())

    @pydoc(categories="logic")
    def or_(self, *others: Any) -> Expr:
        """
        Bitwise `or` operator with the given expressions.
        It can be used with integer and bool types.

        Args:
            others: expressions to peform the bitwise `or` with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").or_(1).alias("or"))
        >>>
        ┌──────┬──────┐
        │ val  ┆ or   │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 1    │
        │ 15   ┆ 15   │
        │ 18   ┆ 19   │
        │ 60   ┆ 61   │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.or_(td_translator._unwrap_into_tdexpr(*others)))

    @pydoc(categories="numeric")
    def pow(self, exponent: td_typing.IntoExprColumn | int | float) -> Expr:
        """
        Exponentiation operator.

        Args:
            exponent: exponent value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").pow(2).alias("pow"))
        >>>
        ┌──────┬──────┐
        │ age  ┆ pow  │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 1    │
        │ 15   ┆ 225  │
        │ 18   ┆ 324  │
        │ 60   ┆ 3600 │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.pow(td_translator._unwrap_into_tdexpr_column(exponent)))

    @pydoc(categories="numeric")
    def radians(self) -> Expr:
        """
        Convert a degree value to radians

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col("val"), td.col("val").radians().alias("radians"))
        >>>
        ┌─────────┬──────────┐
        │ val     ┆ radians  │
        │ ---     ┆ ---      │
        │ i64     ┆ f64      │
        ╞═════════╪══════════╡
        │ 1       ┆ 0.017453 │
        │ 15      ┆ 0.261799 │
        │ 60      ┆ 1.047198 │
        │ 75      ┆ 1.308997 │
        │ null    ┆ null     │
        └─────────┴──────────┘
        """
        return Expr(self._expr.radians())

    @pydoc(categories="aggregation")
    def rank(
        self,
        method: td_typing.RankMethod = "average",
        *,
        descending: bool = False,
        seed: int | None = None,
    ) -> Expr:
        """
        Compute the rank of the element values. Multiple rank types are available.

        Args:
            method: the ranking type: 'average' (default), 'dense', 'max', 'min',
                    'ordinal' or 'random'.
            descending: if the order is ascending (default) or descending.
            seed: random seed when using 'random' rank type.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").rank("max").alias("rank"))
        >>>
        ┌──────┬──────┐
        │ val  ┆ rank │
        │ ---  ┆ ---  │
        │ f64  ┆ u32  │
        ╞══════╪══════╡
        │ -1.0 ┆ 1    │
        │ 0.0  ┆ 2    │
        │ 1.1  ┆ 3    │
        │ 2.0  ┆ 4    │
        │ inf  ┆ 5    │
        │ null ┆ null │
        │ NaN  ┆ 6    │
        └──────┴──────┘
        """
        return Expr(self._expr.rank(method=method, descending=descending, seed=seed))

    @pydoc(categories="numeric")
    def diff(self, n: int = 1) -> Expr:
        """
        Compute the difference between an element value and the element value
        of the specified relative row.

        It supports numberic and datetime types.

        Args:
            n: The relative row to compute the difference with.
               Defaults to 1 (previous). Use a negative number to get a next row.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("i"), td.col("i").diff().diff().alias("diff"))
        >>>
        ┌──────┬──────┐
        │ i    ┆ diff │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ null │
        │ 0    ┆ -1   │
        │ 2    ┆ 2    │
        │ 3    ┆ 1    │
        │ 4    ┆ 1    │
        │ -1   ┆ -5   │
        │ -2   ┆ -1   │
        │ -3   ┆ -1   │
        │ -4   ┆ -1   │
        │ -5   ┆ -1   │
        │ null ┆ null │
        └──────┴──────┘
        """
        # Dropping 'null_behavior' as it is breaks when more than one column (we have
        # system columns)
        return Expr(self._expr.diff(n))

    @pydoc(categories="numeric")
    def reinterpret(self, *, signed: bool = True) -> Expr:
        """
        Reinterpret the 64bit element values (i64 or u64) as a signed/unsigned integers.
        Only valid for 64bit integers, for other types use cast.

        Args:
            signed: `true` to convert to i64, `false` to convert to u64.
                    This named argument must be specified.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val")
        >>>   .reinterpret(signed=False).alias("reinterpret"))
        >>>
        ┌──────┬─────────────┐
        │ val  ┆ reinterpret │
        │ ---  ┆ ---         │
        │ i64  ┆ u64         │
        ╞══════╪═════════════╡
        │ 3    ┆ 3           │
        │ 1    ┆ 1           │
        │ 5    ┆ 5           │
        │ 4    ┆ 4           │
        │ 2    ┆ 2           │
        │ 6    ┆ 6           │
        │ null ┆ null        │
        └──────┴─────────────┘
        """
        return Expr(self._expr.reinterpret(signed=signed))

    @pydoc(categories="numeric")
    def round(self, decimals: int = 0) -> Expr:
        """
        Round floating point element values.

        Args:
            decimals: number of decimal places to round to.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").round().alias("round"))
        >>>
        ┌──────┬───────┐
        │ val  ┆ round │
        │ ---  ┆ ---   │
        │ f64  ┆ f64   │
        ╞══════╪═══════╡
        │ -1.0 ┆ -1.0  │
        │ 0.0  ┆ 0.0   │
        │ 1.1  ┆ 1.0   │
        │ 2.0  ┆ 2.0   │
        │ inf  ┆ inf   │
        │ null ┆ null  │
        │ NaN  ┆ NaN   │
        └──────┴───────┘
        """
        return Expr(self._expr.round(decimals))

    @pydoc(categories="numeric")
    def round_sig_figs(self, digits: int) -> Expr:
        """
        Round floating point element values to the specified significant figures.

        Args:
            digits: number of digits to round up.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").round_sig_figs(2)
        >>>   .alias("round_sig_figs"))
        >>>
        ┌────────┬────────────────┐
        │ val    ┆ round_sig_figs │
        │ ---    ┆ ---            │
        │ f64    ┆ f64            │
        ╞════════╪════════════════╡
        │ 0.0123 ┆ 0.012          │
        │ 2.0244 ┆ 2.0            │
        │ 0.0    ┆ 0.0            │
        │ inf    ┆ NaN            │
        │ 50.0   ┆ 50.0           │
        │ 1.0    ┆ 1.0            │
        │ NaN    ┆ NaN            │
        │ null   ┆ null           │
        │ 112.0  ┆ 110.0          │
        │ 2142.0 ┆ 2100.0         │
        └────────┴────────────────┘
        """
        return Expr(self._expr.round_sig_figs(digits))

    @deprecated(
        "Method 'shrink_dtype' is deprecated.",
        category=deprecation(
            reason=(
                "It had no practical effect on expressions, because data type "
                "shrinking only makes sense on materialized data."
            ),
            since="1.5.0",
            replacement="No direct replacement. Implementation is left as no-op.",
        ),
    )
    @pydoc(categories="numeric")
    def shrink_dtype(self) -> Expr:
        """
        Cast down a column to the smallest type that can hold the element values.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").shrink_dtype()
        >>>   .alias("shrink_dtype"))
        >>>
        ┌───────┬──────────────┐
        │ val   ┆ shrink_dtype │
        │ ---   ┆ ---          │
        │ i64   ┆ i32          │
        ╞═══════╪══════════════╡
        │ 0     ┆ 0            │
        │ 256   ┆ 256          │
        │ 65025 ┆ 65025        │
        └───────┴──────────────┘
        """
        return Expr(self._expr.shrink_dtype())

    @pydoc(categories="numeric")
    def sign(self) -> Expr:
        """
        Calculate the sign of element values.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").sign().alias("sign"))
        >>>
        ┌────────┬──────┐
        │ val    ┆ sign │
        │ ---    ┆ ---  │
        │ f64    ┆ f64  │
        ╞════════╪══════╡
        │ 0.0123 ┆ 1.0  │
        │ 2.0244 ┆ 1.0  │
        │ 0.0    ┆ 0.0  │
        │ inf    ┆ 1.0  │
        │ -50.0  ┆ -1.0 │
        │ 1.0    ┆ 1.0  │
        │ NaN    ┆ NaN  │
        │ null   ┆ null │
        │ -112.0 ┆ -1.0 │
        │ 2142.0 ┆ 1.0  │
        └────────┴──────┘
        """
        return Expr(self._expr.sign())

    @pydoc(categories="numeric")
    def sin(self) -> Expr:
        """
        Calculate the sine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").sin().alias("sin"))
        >>>
        ┌─────┬───────────┐
        │ val ┆ sin       │
        │ --- ┆ ---       │
        │ i64 ┆ f64       │
        ╞═════╪═══════════╡
        │ 0   ┆ 0.0       │
        │ 30  ┆ -0.988032 │
        │ 60  ┆ -0.304811 │
        │ 90  ┆ 0.893997  │
        └─────┴───────────┘

        """
        return Expr(self._expr.sin())

    @pydoc(categories="numeric")
    def sinh(self) -> Expr:
        """
        Calculate the hyperbolic sine of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").sinh().alias("sinh"))
        >>>
        ┌─────┬───────────┐
        │ val ┆ sinh      │
        │ --- ┆ ---       │
        │ i64 ┆ f64       │
        ╞═════╪═══════════╡
        │ 0   ┆ 0.0       │
        │ 30  ┆ 5.3432e12 │
        │ 60  ┆ 5.7100e25 │
        │ 90  ┆ 6.1020e38 │
        └─────┴───────────┘
        """
        return Expr(self._expr.sinh())

    @pydoc(categories="aggregation")
    def count(self) -> Expr:
        """
        Aggregation operation that counts the non `null` values of the given column in
        the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 2    │
        │ A    ┆ 3    │
        │ B    ┆ 0    │
        │ C    ┆ 5    │
        │ null ┆ 6    │
        │ C    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).count().alias("count"))
        >>>
        ┌──────┬───────┐
        │ a    ┆ count │
        │ ---  ┆ ---   │
        │ str  ┆ u32   │
        ╞══════╪═══════╡
        │ null ┆ 1     │
        │ A    ┆ 2     │
        │ B    ┆ 2     │
        │ C    ┆ 1     │
        └──────┴───────┘
        """
        return Expr(self._expr.count())

    @pydoc(categories="aggregation")
    def len(self) -> Expr:
        """
        Aggregation operation that counts the rows in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 2    │
        │ A    ┆ 3    │
        │ B    ┆ 0    │
        │ C    ┆ 5    │
        │ null ┆ 6    │
        │ C    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).len().alias("len"))
        >>>
        ┌──────┬─────┐
        │ a    ┆ len │
        │ ---  ┆ --- │
        │ str  ┆ u32 │
        ╞══════╪═════╡
        │ null ┆ 1   │
        │ A    ┆ 2   │
        │ B    ┆ 2   │
        │ C    ┆ 2   │
        └──────┴─────┘
        """
        return Expr(self._expr.len())

    @pydoc(categories="filters")
    def slice(self, offset: int | Expr, length: int | Expr | None = None) -> Expr:
        """
        Compute a slice of the `TableFrame` for the specified columns.

        Args:
            offset: the offset to start the slice.
            length: the length of the slice.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.select(tf.all().slice(1,2))
        >>>
        ┌─────┬─────┐
        │ x   ┆ y   │
        │ --- ┆ --- │
        │ f64 ┆ f64 │
        ╞═════╪═════╡
        │ 2.0 ┆ 2.0 │
        │ NaN ┆ NaN │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return Expr(
            self._expr.slice(
                td_translator._unwrap_tdexpr(offset),
                td_translator._unwrap_tdexpr(length),
            )
        )

    @pydoc(categories="numeric")
    def sqrt(self) -> Expr:
        """
        Calculate the square root of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").sqrt().alias("sqrt"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ sqrt     │
        │ ---  ┆ ---      │
        │ f64  ┆ f64      │
        ╞══════╪══════════╡
        │ 1.0  ┆ 1.0      │
        │ 2.0  ┆ 1.414214 │
        │ NaN  ┆ NaN      │
        │ 4.0  ┆ 2.0      │
        │ 5.0  ┆ 2.236068 │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        return Expr(self._expr.sqrt())

    @pydoc(categories="numeric")
    def sub(self, other: Any) -> Expr:
        """
        Equivalent to the `-` operator.

        Args:
            other: value to subtract.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").sub(1).alias("sub"))
        >>>
        ┌──────┬──────────┐
        │ val  ┆ sub      │
        │ ---  ┆ ---      │
        │ i64  ┆ i64      │
        ╞══════╪══════════╡
        │ 1    ┆  0       │
        │ 15   ┆ 14       │
        │ 18   ┆ 17       │
        │ 60   ┆ 59       │
        │ 60   ┆ 59       │
        │ 75   ┆ 74       │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.sub(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="aggregation")
    def max(self) -> Expr:
        """
        Aggregation operation that finds the maximum value in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).max())
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ F    ┆ -5   │
        │ C    ┆ -1   │
        │ A    ┆ 2    │
        │ B    ┆ 4    │
        │ D    ┆ -4   │
        │ null ┆ null │
        └──────┴──────┘
        """
        return Expr(self._expr.max())

    @pydoc(categories="aggregation")
    def min(self) -> Expr:
        """
        Aggregation operation that finds the minimum value in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).min())
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ C    ┆ -3   │
        │ A    ┆ 1    │
        │ null ┆ null │
        │ B    ┆ 0    │
        │ F    ┆ -5   │
        │ D    ┆ -4   │
        └──────┴──────┘
        """
        return Expr(self._expr.min())

    @pydoc(categories="aggregation")
    def sum(self) -> Expr:
        """
        Aggregation operation that sums the values in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).sum())
        >>>
        ┌──────┬─────┐
        │ ss   ┆ i   │
        │ ---  ┆ --- │
        │ str  ┆ i64 │
        ╞══════╪═════╡
        │ null ┆ 0   │
        │ A    ┆ 3   │
        │ B    ┆ 7   │
        │ C    ┆ -6  │
        │ D    ┆ -4  │
        │ F    ┆ -5  │
        └──────┴─────┘
        """
        return Expr(self._expr.sum())

    @pydoc(categories="aggregation")
    def mean(self) -> Expr:
        """
        Aggregation operation that finds the mean of the values in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b").mean())
        >>>
        ┌──────┬──────────┐
        │ ss   ┆ i        │
        │ ---  ┆ ---      │
        │ str  ┆ f64      │
        ╞══════╪══════════╡
        │ null ┆ null     │
        │ A    ┆ 1.5      │
        │ F    ┆ -5.0     │
        │ C    ┆ -2.0     │
        │ D    ┆ -4.0     │
        │ B    ┆ 2.333333 │
        └──────┴──────────┘
        """
        return Expr(self._expr.mean())

    @pydoc(categories="aggregation")
    def median(self) -> Expr:
        """
        Aggregation operation that finds the median of the values in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).median())
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ f64  │
        ╞══════╪══════╡
        │ F    ┆ -5.0 │
        │ C    ┆ -2.0 │
        │ B    ┆ 3.0  │
        │ D    ┆ -4.0 │
        │ A    ┆ 1.5  │
        │ null ┆ null │
        └──────┴──────┘
        """
        return Expr(self._expr.median())

    @pydoc(categories="aggregation")
    def n_unique(self) -> Expr:
        """
        Aggregation operation that counts the unique values of the given column
        in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ ss   ┆ i    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ B    ┆ 0    │
        │ A    ┆ 2    │
        │ B    ┆ 3    │
        │ B    ┆ 4    │
        │ C    ┆ -1   │
        │ C    ┆ -2   │
        │ C    ┆ -3   │
        │ D    ┆ -4   │
        │ F    ┆ -5   │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b")).n_unique())
        >>>
        ┌──────┬─────┐
        │ ss   ┆ i   │
        │ ---  ┆ --- │
        │ str  ┆ u32 │
        ╞══════╪═════╡
        │ D    ┆ 1   │
        │ C    ┆ 3   │
        │ A    ┆ 2   │
        │ B    ┆ 3   │
        │ F    ┆ 1   │
        │ null ┆ 1   │
        └──────┴─────┘
        """
        return Expr(self._expr.n_unique())

    @pydoc(categories="numeric")
    def tan(self) -> Expr:
        """
        Calculate the tangent of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").tan().alias("tan"))
        >>>
        ┌─────┬───────────┐
        │ val ┆ tan       │
        │ --- ┆ ---       │
        │ i64 ┆ f64       │
        ╞═════╪═══════════╡
        │ 0   ┆ 0.0       │
        │ 30  ┆ -6.405331 │
        │ 60  ┆ 0.32004   │
        │ 90  ┆ -1.9952   │
        └─────┴───────────┘
        """
        return Expr(self._expr.tan())

    @pydoc(categories="numeric")
    def tanh(self) -> Expr:
        """
        Calculate the hyperbolic tangent of the element value.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").tanh().alias("tanh"))
        >>>
        ┌─────┬──────────┐
        │ val ┆ tanh     │
        │ --- ┆ ---      │
        │ f64 ┆ f64      │
        ╞═════╪══════════╡
        │ 0.0 ┆ 0.0      │
        │ 3.0 ┆ 0.995055 │
        │ 6.0 ┆ 0.999988 │
        │ 9.0 ┆ 1.0      │
        └─────┴──────────┘
        """
        return Expr(self._expr.tanh())

    @pydoc(categories="numeric")
    def truediv(self, other: Any) -> Expr:
        """
        Equivalent to the float `/` operator.

        Args:
            other: value to divide by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").truediv(3).alias("truediv"))
        >>>
        ┌────────┬────────────┐
        │ val    ┆ truediv    │
        │ ---    ┆ ---        │
        │ f64    ┆ f64        │
        ╞════════╪════════════╡
        │ 0.0123 ┆ 0.0041     │
        │ 2.0244 ┆ 0.6748     │
        │ 0.0    ┆ 0.0        │
        │ inf    ┆ inf        │
        │ -50.0  ┆ -16.666667 │
        │ 1.0    ┆ 0.333333   │
        │ NaN    ┆ NaN        │
        │ null   ┆ null       │
        │ -112.0 ┆ -37.333333 │
        │ 2142.0 ┆ 714.0      │
        └────────┴────────────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.truediv(td_translator._unwrap_into_tdexpr(other)))

    @pydoc(categories="logic")
    def xor(self, other: Any) -> Expr:
        """
        Bitwise `xor` operator with the given expression.
        It can be used with integer and bool types.

        Args:
            other: expression to peform the bitwise `xor` with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("val"), td.col("val").xor(8).alias("xor"))
        >>>
        ┌─────┬─────┐
        │ val ┆ xor │
        │ --- ┆ --- │
        │ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 0   ┆ 8   │
        │ 30  ┆ 22  │
        │ 60  ┆ 52  │
        │ 90  ┆ 82  │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return Expr(self._expr.xor(td_translator._unwrap_into_tdexpr(other)))

    """ Object Properties - NameSpaces """

    @pydoc(categories="type_casting")
    @property
    def dt(self) -> td_datetime.ExprDateTimeNameSpace:
        """
        Return an object namespace with all date-time methods for a date-time value.
        """
        return td_datetime.ExprDateTimeNameSpace(self._expr.dt)

    @pydoc(categories="type_casting")
    @property
    def str(self) -> td_string.ExprStringNameSpace:
        """
        Return an object namespace with all string methods for a string value.
        """
        return td_string.ExprStringNameSpace(self._expr.str)


def expr_resolves_to_multiple_outputs(expr: pl.Expr) -> bool:
    try:
        return expr._pyexpr.meta_has_multiple_outputs()
    except AttributeError:
        return False
