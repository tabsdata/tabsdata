#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from collections.abc import Iterable

import polars as pl
from polars.lazyframe.group_by import LazyGroupBy

import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._constants as td_constants

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._helpers as td_helpers

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._translator as td_translator
from tabsdata.exceptions import ErrorCode, TableFrameError


class TdLazyGroupBy:
    def __init__(self, lgb: LazyGroupBy | TdLazyGroupBy) -> None:
        if isinstance(lgb, LazyGroupBy):
            self._lgb = lgb
        elif isinstance(lgb, TdLazyGroupBy):
            self._lgb = lgb._lgb
        else:
            raise TableFrameError(ErrorCode.TF6, type(lgb))

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    def agg(
        self,
        *aggs: td_expr.IntoTdExpr | Iterable[td_expr.IntoTdExpr],
        **named_aggs: td_expr.IntoTdExpr,
    ) -> td_frame.TdLazyFrame:
        """
        Aggregation expressions for the group by column(s).

        Args:
            *aggs: Aggregation operations.

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
        >>> tf.group_by(td.col("a")).agg(td.col("b").sum().alias("agg")).collect()
        >>>
        ┌──────┬─────┐
        │ a    ┆ agg │
        │ ---  ┆ --- │
        │ str  ┆ i64 │
        ╞══════╪═════╡
        │ A    ┆ 4   │
        │ C    ┆ 5   │
        │ null ┆ 6   │
        │ B    ┆ 2   │
        └──────┴─────┘
        """
        unwrapped_aggs = []
        for agg in aggs:
            # noinspection PyProtectedMember
            result = td_translator._unwrap_into_tdexpr(agg)
            unwrapped_aggs.append(result)
        unwrapped_named_aggs = []

        for named_agg in named_aggs:
            # noinspection PyProtectedMember
            result = td_translator._unwrap_into_tdexpr(named_agg)
            print(f"Processing named_agg: {named_agg} -> {result}")  # Debugging output
            unwrapped_named_aggs.append(result)

        unwrapped_required_columns = []
        for col, metadata in td_helpers.REQUIRED_COLUMNS_METADATA.items():
            if metadata[td_constants.TD_COL_DTYPE] == pl.List:
                expr = pl.col(col).explode().alias(col)
            else:
                expr = pl.col(col).first()
            # noinspection PyProtectedMember
            result = td_translator._unwrap_into_tdexpr(expr)
            unwrapped_required_columns.append(result)

        expressions = unwrapped_aggs + unwrapped_required_columns + unwrapped_named_aggs

        return td_frame.TdLazyFrame.__build__(
            self._lgb.agg(
                expressions,
            )
        )

    def len(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that counts the rows in the group.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by(td.col("ss")).len()
        >>>
        ┌──────┬─────┐
        │ ss   ┆ len │
        │ ---  ┆ --- │
        │ str  ┆ u32 │
        ╞══════╪═════╡
        │ null ┆ 1   │
        │ B    ┆ 3   │
        │ F    ┆ 1   │
        │ C    ┆ 3   │
        │ A    ┆ 2   │
        │ D    ┆ 1   │
        └──────┴─────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.len())

    def count(self) -> td_frame.TdLazyFrame:
        return td_frame.TdLazyFrame.__build__(self._lgb.count())

    def max(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that computes the maximum value in the group for
        of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").max()
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ null ┆ null ┆ null │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 7    ┆ -1.1 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        └──────┴──────┴──────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.max())

    def mean(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that computes the mean value in the group for
        of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").mean()
        >>>
        ┌──────┬──────────┬──────────┐
        │ ss   ┆ u        ┆ ff       │
        │ ---  ┆ ---      ┆ ---      │
        │ str  ┆ f64      ┆ f64      │
        ╞══════╪══════════╪══════════╡
        │ null ┆ null     ┆ null     │
        │ A    ┆ 1.5      ┆ 1.65     │
        │ B    ┆ 2.333333 ┆ 2.566667 │
        │ C    ┆ 6.0      ┆ -2.2     │
        │ D    ┆ 8.0      ┆ inf      │
        │ F    ┆ 9.0      ┆ NaN      │
        └──────┴──────────┴──────────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.mean())

    def median(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that computes the median value in the group for
        of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").median()
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ f64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ null ┆ null ┆ null │
        │ A    ┆ 1.5  ┆ 1.65 │
        │ B    ┆ 3.0  ┆ 3.3  │
        │ C    ┆ 6.0  ┆ -2.2 │
        │ D    ┆ 8.0  ┆ inf  │
        │ F    ┆ 9.0  ┆ NaN  │
        └──────┴──────┴──────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.median())

    def min(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that computes the minimum value in the group for
        of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").min()
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ null ┆ null ┆ null │
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ C    ┆ 5    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        └──────┴──────┴──────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.min())

    def n_unique(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that counts the unique values of the given column
        in the group for of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").n_unique()
        >>>
        ┌──────┬─────┬─────┐
        │ ss   ┆ u   ┆ ff  │
        │ ---  ┆ --- ┆ --- │
        │ str  ┆ u32 ┆ u32 │
        ╞══════╪═════╪═════╡
        │ null ┆ 1   ┆ 1   │
        │ A    ┆ 2   ┆ 2   │
        │ B    ┆ 3   ┆ 3   │
        │ C    ┆ 3   ┆ 3   │
        │ D    ┆ 1   ┆ 1   │
        │ F    ┆ 1   ┆ 1   │
        └──────┴─────┴─────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.n_unique())

    def sum(self) -> td_frame.TdLazyFrame:
        """
        Aggregation operation that computes the sum for all values in the group for
        of all the non `group by` columns.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.group_by("ss").sum()
        >>>
        ┌──────┬─────┬──────┐
        │ ss   ┆ u   ┆ ff   │
        │ ---  ┆ --- ┆ ---  │
        │ str  ┆ i64 ┆ f64  │
        ╞══════╪═════╪══════╡
        │ null ┆ 0   ┆ 0.0  │
        │ A    ┆ 3   ┆ 3.3  │
        │ B    ┆ 7   ┆ 7.7  │
        │ C    ┆ 18  ┆ -6.6 │
        │ D    ┆ 8   ┆ inf  │
        │ F    ┆ 9   ┆ NaN  │
        └──────┴─────┴──────┘
        """
        return td_frame.TdLazyFrame.__build__(self._lgb.sum())
