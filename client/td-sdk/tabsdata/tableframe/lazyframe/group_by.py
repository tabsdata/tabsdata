#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

import polars as pl
import polars.lazyframe.group_by as pl_group_by
from polars import functions

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._helpers as td_helpers

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator
import tabsdata.tableframe.lazyframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing

# noinspection PyProtectedMember
from tabsdata._utils.annotations import pydoc
from tabsdata.exceptions import ErrorCode, TableFrameError

STATE = "_state"


class TableFrameGroupBy:
    @contextmanager
    def _transient_state(self, attribute, temp_value):
        original_value = getattr(self, attribute)
        setattr(self, attribute, temp_value)
        try:
            yield
        finally:
            setattr(self, attribute, original_value)

    # noinspection PyUnreachableCode
    def __init__(self, lgb: pl_group_by.LazyGroupBy | TableFrameGroupBy) -> None:
        if isinstance(lgb, pl_group_by.LazyGroupBy):
            self._lgb = lgb
        elif isinstance(lgb, TableFrameGroupBy):
            self._lgb = lgb._lgb
        else:
            raise TableFrameError(ErrorCode.TF6, type(lgb))
        self._state = None

    def agg(
        self,
        *aggs: td_typing.IntoExpr | Iterable[td_typing.IntoExpr],
        **named_aggs: td_typing.IntoExpr,
    ) -> td_frame.TableFrame:
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
            unwrapped_named_aggs.append(result)

        system_agg = []
        for column, metadata in td_helpers.REQUIRED_COLUMNS_METADATA.items():
            if metadata.aggregation is not None:
                aggregation = metadata.aggregation
                aggregation_instance = aggregation(
                    column,
                    self._state,
                )
                # In polars 1.31.0, map_elements internally set agg_list=True
                #    which aggregated values into a list before calling the
                #     map function.
                #
                # In polars 1.32.0, this was removed, so the map function
                #     receives individual values (as integers) instead of
                #     list[bytes].
                #
                # Using implode() restores back the expected behaviour.
                #
                # Eventually whe should move to map_batches, that gives direct
                #     access to the agg_list flag.
                expr = (
                    pl.col(column)
                    .implode()
                    .map_batches(
                        aggregation_instance.python,
                        return_dtype=metadata.dtype,
                        returns_scalar=True,
                    )
                    .alias(column)
                )
            else:
                # As TableFrame instantiation mode is 'tab', we do not need to enforce
                # adding the non-aggregation system columns.
                # expr = pl.col(column).first()
                expr = None
            if expr is not None:
                # noinspection PyProtectedMember
                result = td_translator._unwrap_into_tdexpr(expr)
                system_agg.append(result)

        expressions = unwrapped_aggs + system_agg + unwrapped_named_aggs

        return td_frame.TableFrame.__build__(
            df=self._lgb.agg(expressions),
            mode="tab",
            idx=None,
            properties=None,
        )

    def len(self) -> td_frame.TableFrame:
        """
        Aggregation operation that counts the rows in the group.
        All the columns of the original `TableFrame` are included in the result,
        with the count in each of thse columns.

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
        ┌──────┬─────┬─────┐
        │ ss   ┆ u   │ ff  │
        │ ---  ┆ --- │ --- │
        │ str  ┆ u32 │ f64 │
        ╞══════╪═════╡═════│
        │ A    ┆ 2   │ 2   │
        │ B    ┆ 3   │ 3   │
        │ C    ┆ 3   │ 3   │
        │ D    ┆ 1   │ 1   │
        │ F    ┆ 1   │ 1   │
        │ null ┆ 1   │ 1   │
        └──────┴─────┴─────┘
        """
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_LEN):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).len()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def count(self) -> td_frame.TableFrame:
        """
        Aggregation operation that counts the non-null rows in the group.
        All the columns of the original `TableFrame` are included in the result,
        with the count in each of thse columns.

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
        >>> tf.group_by(td.col("ss")).count()
        >>>
        ┌──────┬─────┬─────┐
        │ ss   ┆ u   ┆ ff  │
        │ ---  ┆ --- ┆ --- │
        │ str  ┆ u32 ┆ u32 │
        ╞══════╪═════╪═════╡
        │ A    ┆ 2   ┆ 2   │
        │ B    ┆ 3   ┆ 3   │
        │ C    ┆ 3   ┆ 3   │
        │ D    ┆ 1   ┆ 1   │
        │ F    ┆ 1   ┆ 1   │
        │ null ┆ 0   ┆ 0   │
        └──────┴─────┴─────┘
        """
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_COUNT):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).count()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def max(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_MAX):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).max()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def mean(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_MEEAN):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).mean()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def median(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_MEDIAN):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).median()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def min(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_MIN):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).min()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def n_unique(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_UNIQUE):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).n_unique()),
                mode="tab",
                idx=None,
                properties=None,
            )

    @pydoc(categories="aggregation")
    def sum(self) -> td_frame.TableFrame:
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
        with self._transient_state(STATE, td_constants.RowOperation.GROUP_SUM):
            return td_frame.TableFrame.__build__(
                df=self.agg(functions.all().exclude(system_agg_columns()).sum()),
                mode="tab",
                idx=None,
                properties=None,
            )


def system_agg_columns() -> list[str]:
    return [
        column
        for column, metadata in td_helpers.REQUIRED_COLUMNS_METADATA.items()
        if metadata.aggregation is not None
    ]
