#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from collections.abc import Collection, Iterable, Mapping, Sequence
from typing import Any, List, NoReturn, TypeVar

import polars as pl
from accessify import accessify, private
from polars import DataType, Schema, Series

# noinspection PyProtectedMember
from polars._typing import ColumnNameOrSelector, JoinStrategy, UniqueKeepStrategy
from polars.dependencies import numpy as np

import tabsdata as td

# noinspection PyProtectedMember
import tabsdata.tableframe._typing as td_typing
import tabsdata.tableframe.dataframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.group_by as td_group_by

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._common as td_common

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._constants as td_constants

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._generators as td_generators

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._helpers as td_helpers

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._reflection as td_reflection

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._translator as td_translator
from tabsdata.exceptions import ErrorCode, TableFrameError
from tabsdata.utils.annotations import pydoc, unstable

# noinspection PyProtectedMember
from td_interceptor.interceptor import Interceptor

# ToDo: SDK-128: Define the logging model for SDK CLI execution
logger = logging.getLogger(__name__)


@accessify
class TableFrame:
    """Owned Functions."""

    @classmethod
    def _from_lazy(cls, lf: pl.LazyFrame) -> TableFrame:
        return TableFrame.__build__(lf)

    def _to_lazy(self) -> pl.LazyFrame:
        return self._lf

    """ Initialization Functions """

    @classmethod
    @pydoc(categories="tableframe")
    def empty(cls) -> TableFrame:
        return TableFrame.__build__(None)

    @classmethod
    def __build__(
        cls,
        df: (
            td_typing.TableDictionary | pl.LazyFrame | pl.DataFrame | TableFrame | None
        ) = None,
    ) -> TableFrame:
        # noinspection PyProtectedMember
        if df is None:
            df = pl.LazyFrame(None)
        elif isinstance(df, dict):
            df = pl.LazyFrame(df)
        elif isinstance(df, pl.LazyFrame):
            pass
        elif isinstance(df, pl.DataFrame):
            df = df.lazy()
        elif isinstance(df, TableFrame):
            df = df._lf
        else:
            raise TableFrameError(ErrorCode.TF2, type(df))

        instance = cls.__new__(cls)

        # noinspection PyProtectedMember
        instance._id = td_generators._id()
        df = td_common.add_system_columns(df)
        instance._lf = df
        return instance

    def __init__(
        self,
        df: td_typing.TableDictionary | TableFrame | None = None,
    ) -> None:
        if isinstance(df, TableFrame):
            # noinspection PyProtectedMember
            df = df._lf
        else:
            if df is None:
                df = pl.LazyFrame(None)
            elif isinstance(df, dict):
                df = pl.LazyFrame(df)
            else:
                raise TableFrameError(ErrorCode.TF2, type(df))
            df = td_common.add_system_columns(df)

        td_reflection.check_required_columns(df)

        # noinspection PyProtectedMember
        self._id = td_generators._id()
        self._lf = df

    @property
    def columns(self) -> list[str]:
        return self._lf.collect_schema().names()

    @pydoc(categories="attributes")
    @property
    def dtypes(self) -> list[DataType]:
        return self._lf.collect_schema().dtypes()

    @property
    def schema(self) -> Schema:
        return self._lf.collect_schema()

    @property
    def width(self) -> int:
        return self.schema.len()

    """ Special Functions """

    # ToDo: pending restricted access and system td columns handling.
    # status(Status.TODO)
    def __getattr__(self, name):
        if name in self._lf.__dict__:
            attr = getattr(self._lf, name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if isinstance(result, pl.LazyFrame):
                        return TableFrame.__build__(result)
                    return result

                return wrapper
            return attr
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __bool__(self) -> NoReturn:
        return self._lf.__bool__()

    def __eq__(self, other: object) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._id == other._id
        else:
            return self._lf.__eq__(other=other)

    def __ne__(self, other: object) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._id != other._id
        else:
            return self._lf.__ne__(other=other)

    def __gt__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__gt__(other=other._lf)
        else:
            return self._lf.__gt__(other=other)

    def __lt__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__lt__(other=other._lf)
        else:
            return self._lf.__lt__(other=other)

    def __ge__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__ge__(other=other._lf)
        else:
            return self._lf.__ge__(other=other)

    def __le__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__le__(other=other._lf)
        else:
            return self._lf.__le__(other=other)

    # ToDo: should we block system td columns?
    def __contains__(self, key: str) -> bool:
        return self._lf.__contains__(key=key)

    def __copy__(self) -> TableFrame:
        return TableFrame.__build__(self._lf.__copy__())

    def __deepcopy__(self, memo: None = None) -> TableFrame:
        return TableFrame.__build__(self._lf.__deepcopy__(memo=memo))

    def __getitem__(self, item: int | range | slice) -> TableFrame:
        return TableFrame.__build__(self._lf.__getitem__(item=item))

    def __str__(self) -> str:
        return self._lf.explain(optimized=False)

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} at 0x{id(self):X}> with {self._lf.__repr__()}"
        ).replace("LazyFrame", "TableFrame")

    @private
    def _repr_html_(self) -> str:
        # noinspection PyProtectedMember
        return self._lf._repr_html_().replace("LazyFrame", "TableFrame")

    """ Description Functions """

    @private
    @pydoc(categories="description")
    def explain(self) -> None:
        logger.info(
            self._lf.explain(
                format="plain",
                optimized=True,
                type_coercion=True,
                predicate_pushdown=True,
                projection_pushdown=True,
                simplify_expression=True,
                slice_pushdown=True,
                comm_subplan_elim=True,
                comm_subexpr_elim=True,
                cluster_with_columns=True,
                collapse_joins=True,
                streaming=False,
                tree_format=None,
            )
        )

    @private
    @pydoc(categories="description")
    def show_graph(self) -> str | None:
        logger.info(
            self._lf.show_graph(
                optimized=True,
                show=True,
                output_path=None,
                raw_output=False,
                figsize=(16.0, 12.0),
                type_coercion=True,
                predicate_pushdown=True,
                projection_pushdown=True,
                simplify_expression=True,
                slice_pushdown=True,
                comm_subplan_elim=True,
                comm_subexpr_elim=True,
                cluster_with_columns=True,
                collapse_joins=True,
                streaming=False,
            )
        )

    @private
    def inspect(self, fmt: str = "{}") -> TableFrame:
        return TableFrame.__build__(self._lf.inspect(fmt=fmt))

    """ Transformation Functions """

    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="tableframe")
    def sort(
        self,
        by: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        *more_by: td_expr.IntoExpr,
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] = False,
        maintain_order: bool = False,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Sort the `TableFrame` by the given column(s) or expression(s).

        Args:
            by: Column(s) or expression(s) to sort by.
            more_by: Additional colums to sort by.
            descending: Specifies if the sorting should be descending or not.
            nulls_last: Specifies if `null` values should be placed last.
            maintain_order: Preserve the order of equal rows.

        Example:

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
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.sort(td.col("a"), descending = True)
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        │ A    ┆ 1.0   │
        │ X    ┆ 10.0  │
        │ C    ┆ 3.0   │
        │ D    ┆ 5.0   │
        │ M    ┆ 9.0   │
        │ A    ┆ 100.0 │
        │ M    ┆ 50.0  │
        │ null ┆ 20.0  │
        │ F    ┆ null  │
        └──────┴───────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.sort(
                by=td_translator._unwrap_into_tdexpr([by] + list(more_by)),
                *more_by,
                descending=descending,
                nulls_last=nulls_last,
                maintain_order=maintain_order,
                multithreaded=False,
            )
        )

    # ToDo: disallow transformations in system td columns.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def cast(
        self,
        dtypes: (
            Mapping[ColumnNameOrSelector | td_typing.TdDataType, td_typing.TdDataType]
            | td_typing.TdDataType
        ),
        *,
        strict: bool = True,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Cast columns to a new data type.

        Args:
            dtypes: Mapping of the column name(s) to the new data type(s).
            strict: If `True`, raises an error if the cast cannot be performed.

        Example:

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
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.cast({"b":pl.Float32}).collect()
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        │ A    ┆ 1.0   │
        │ X    ┆ 10.0  │
        │ C    ┆ 3.0   │
        │ D    ┆ 5.0   │
        │ M    ┆ 9.0   │
        │ A    ┆ 100.0 │
        │ M    ┆ 50.0  │
        │ null ┆ 20.0  │
        │ F    ┆ null  │
        └──────┴───────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.cast(dtypes=td_translator._unwrap_tdexpr(dtypes), strict=strict)
        )

    # ToDo: should we allow only clear to 0 rows?
    # status(Status.TODO)
    @pydoc(categories="tableframe")
    def clear(self, n: int = 0) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Clears all rows of the `TableFrame` preserving the schema.

        Example:

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
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.cast({"b":pl.Float32}).collect()
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        └──────┴───────┘
        """
        return TableFrame.__build__(self._lf.clear(n=n))

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="join")
    def join(
        self,
        other: TableFrame,
        on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        how: JoinStrategy = "inner",
        *,
        left_on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        right_on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        suffix: str = "_right",
        join_nulls: bool = False,
        coalesce: bool | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Join the `TableFrame` with another `TableFrame`.

        Args:
            other: The `TableFrame` to join.
            on: Name(s) of the columns to join on. The column name(s) must be in
                both `TableFrame's. Don't use this parameter if using `left_on`
                and `right_on` parameters, or if `how="cross"`.
            how: Join strategy:
                * `inner`: An inner join.
                * `left`: A left join.
                * `right`: A rigth join.
                * `full`: A full join.
                * `cross`: The cartesian product.
                * `semi`: An inner join but only returning the columns from left
                          `TableFrame`.
                * *anti*: Rows from the left `TableFrame` that have no match
                          in the right `TableFrame`.
            left_on: Name(s) of the columns to join on from the left `TableFrame`.
                It must be used together wit the `right_on` parameter.
                It cannot be used with the `on` parameter.
            right_on: Name(s) of the columns to join on from the right `TableFrame`.
                It must be used together wit the `left_on` parameter.
                It cannot be used with the `on` parameter.
            suffix: Duplicate columns on the right `Table` are appended this suffix.
            join_nulls: If `null` value matches should produce join rows or not.
            coalesce: Collapse join columns into a single column.

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
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf2: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 3    │
        │ Y    ┆ 4    │
        │ Z    ┆ 5    │
        │ A    ┆ 0    │
        │ M    ┆ 6    │
        │ null ┆ 8    │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        An inner join:
        >>>
        >>> tf1.join(tf2, on="a", how="inner")
        >>>
        ┌─────┬──────┬─────────┐
        │ a   ┆ b    ┆ b_right │
        │ --- ┆ ---  ┆ ---     │
        │ str ┆ i64  ┆ i64     │
        ╞═════╪══════╪═════════╡
        │ A   ┆ 1    ┆ 3       │
        │ A   ┆ 1    ┆ 0       │
        │ M   ┆ 9    ┆ 6       │
        │ A   ┆ 100  ┆ 3       │
        │ A   ┆ 100  ┆ 0       │
        │ M   ┆ 50   ┆ 6       │
        │ F   ┆ null ┆ null    │
        └─────┴──────┴─────────┘
        >>>
        A left join:
        >>>
        >>> tf1.join(tf2, on="a", how="left")
        >>>
        ┌──────┬──────┬─────────┐
        │ a    ┆ b    ┆ b_right │
        │ ---  ┆ ---  ┆ ---     │
        │ str  ┆ i64  ┆ i64     │
        ╞══════╪══════╪═════════╡
        │ A    ┆ 1    ┆ 3       │
        │ A    ┆ 1    ┆ 0       │
        │ X    ┆ 10   ┆ null    │
        │ C    ┆ 3    ┆ null    │
        │ D    ┆ 5    ┆ null    │
        │ …    ┆ …    ┆ …       │
        │ A    ┆ 100  ┆ 3       │
        │ A    ┆ 100  ┆ 0       │
        │ M    ┆ 50   ┆ 6       │
        │ null ┆ 20   ┆ null    │
        │ F    ┆ null ┆ null    │
        └──────┴──────┴─────────┘
        >>>
        Turning off column coalesce:
        >>>
        >>> tf1.join(tf2, on="a", coalesce=False)
        >>>
        ┌─────┬──────┬─────────┬─────────┐
        │ a   ┆ b    ┆ a_right ┆ b_right │
        │ --- ┆ ---  ┆ ---     ┆ ---     │
        │ str ┆ i64  ┆ str     ┆ i64     │
        ╞═════╪══════╪═════════╪═════════╡
        │ A   ┆ 1    ┆ A       ┆ 3       │
        │ A   ┆ 1    ┆ A       ┆ 0       │
        │ M   ┆ 9    ┆ M       ┆ 6       │
        │ A   ┆ 100  ┆ A       ┆ 3       │
        │ A   ┆ 100  ┆ A       ┆ 0       │
        │ M   ┆ 50   ┆ M       ┆ 6       │
        │ F   ┆ null ┆ F       ┆ null    │
        └─────┴──────┴─────────┴─────────┘
        """
        # noinspection PyProtectedMember
        lf = self._lf.join(
            other=other._lf,
            on=td_translator._unwrap_tdexpr(on),
            how=how,
            left_on=td_translator._unwrap_tdexpr(left_on),
            right_on=td_translator._unwrap_tdexpr(right_on),
            suffix=suffix,
            validate="m:m",
            join_nulls=join_nulls,
            coalesce=coalesce,
            allow_parallel=True,
            force_parallel=False,
        )
        return TableFrame.__build__(_assemble_columns(lf))

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def with_columns(
        self,
        *exprs: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        **named_exprs: td_expr.IntoExpr,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Add columns to the `TableFrame`.

        Args:
            exprs: Columns or expressions to add.
            named_exprs: Named expressions to add.

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
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.with_columns(td.col("x").mul(td.col("y")).alias("z"))
        >>>
        ┌──────┬──────┬──────┐
        │ x    ┆ y    ┆ z    │
        │ ---  ┆ ---  ┆ ---  │
        │ f64  ┆ f64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ 1.0  ┆ 2.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  ┆ 4.0  │
        │ NaN  ┆ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  ┆ NaN  │
        │ 5.0  ┆ null ┆ null │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.with_columns(
                *[td_translator._unwrap_into_tdexpr_column(column) for column in exprs],
                **named_exprs,
            )
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def drop(
        self,
        *columns: ColumnNameOrSelector | Iterable[ColumnNameOrSelector],
        strict: bool = True,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Discard columns from the `TableFrame`.

        Args:
            columns: Columns to drop.
            strict: If True, raises an error if a column does not exist.

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
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.drop("y")
        >>>
        ┌──────┐
        │ x    │
        │ ---  │
        │ f64  │
        ╞══════╡
        │ 1.0  │
        │ 2.0  │
        │ NaN  │
        │ 4.0  │
        │ 5.0  │
        │ null │
        └──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.drop(td_translator._unwrap_tdexpr(*columns), strict=strict)
        )

    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def fill_null(
        self,
        value: Any | td_expr.Expr | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Replace all `null` values in the `TableFrame` with the given value.

        Args:
            value: The value to replace `null` with.

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
        >>> tf.fill_null(20)
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
        │ 5.0  ┆ 20.0 │
        │ 20.0 ┆ 20.0 │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.fill_null(
                value=td_translator._unwrap_tdexpr(value),
                strategy=None,
                limit=None,
                matches_supertype=True,
            )
        )

    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def fill_nan(self, value: int | float | td_expr.Expr | None) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Replace all `NaN` values in the `TableFrame` with the given value.

        Args:
            value: The value to replace `NaN` with.

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
        >>> tf.fill_nan(10)
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.fill_nan(value=td_translator._unwrap_tdexpr(value))
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="filters")
    def unique(
        self,
        subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None,
        *,
        keep: UniqueKeepStrategy = "any",
        maintain_order: bool = False,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Deduplicate rows from the `TableFrame`.

        Args:
            subset: Columns to evaluate for duplicate values. If None, all columns are
                considered.
            keep: Strategy to keep duplicates: `first`, `last`, `any`, `none` (
                eliminate duplicate rows).
            maintain_order: Preserve the order of the rows.

        Example:

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
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.unique("a", keep="last")
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ D    ┆ 5    │
        │ C    ┆ 3    │
        │ X    ┆ 10   │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ F    ┆ null │
        │ null ┆ 20   │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.unique(
                subset=td_translator._unwrap_tdexpr(subset),
                keep=keep,
                maintain_order=maintain_order,
            )
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def drop_nans(
        self,
        subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Drop rows with `NaN` values.

        Args:
            subset: Columns to look for `Nan` values. If None, all columns are
                considered.

        Example:

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
        >>> tf.unique("a", keep="last")
        ┌─────┬─────┬──────┐
        │ ss  ┆ u   ┆ ff   │
        │ --- ┆ --- ┆ ---  │
        │ str ┆ i64 ┆ f64  │
        ╞═════╪═════╪══════╡
        │ A   ┆ 1   ┆ 1.1  │
        │ B   ┆ 0   ┆ 0.0  │
        │ A   ┆ 2   ┆ 2.2  │
        │ B   ┆ 3   ┆ 3.3  │
        │ B   ┆ 4   ┆ 4.4  │
        │ C   ┆ 5   ┆ -1.1 │
        │ C   ┆ 6   ┆ -2.2 │
        │ C   ┆ 7   ┆ -3.3 │
        │ D   ┆ 8   ┆ inf  │
        └─────┴─────┴──────┘
        >>>
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.drop_nans(subset=td_translator._unwrap_tdexpr(subset))
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def drop_nulls(
        self,
        subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Drop rows with null values.

        Args:
            subset: Columns to evaluate for null values. If None, all columns are
                considered.

        Example:

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
        │ G    ┆ null ┆ 2.3  │
        └──────┴──────┴──────┘
        >>>
        >>> tf.drop_nulls("a")
        >>>
        ┌─────┬─────┬──────┐
        │ ss  ┆ u   ┆ ff   │
        │ --- ┆ --- ┆ ---  │
        │ str ┆ i64 ┆ f64  │
        ╞═════╪═════╪══════╡
        │ A   ┆ 1   ┆ 1.1  │
        │ B   ┆ 0   ┆ 0.0  │
        │ A   ┆ 2   ┆ 2.2  │
        │ B   ┆ 3   ┆ 3.3  │
        │ B   ┆ 4   ┆ 4.4  │
        │ C   ┆ 5   ┆ -1.1 │
        │ C   ┆ 6   ┆ -2.2 │
        │ C   ┆ 7   ┆ -3.3 │
        │ D   ┆ 8   ┆ inf  │
        │ F   ┆ 9   ┆ NaN  │
        └─────┴─────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.drop_nulls(subset=td_translator._unwrap_tdexpr(subset))
        )

    """Retrieval Functions"""

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    @pydoc(categories="filters")
    def filter(
        self,
        *predicates: (
            td_expr.IntoExprColumn
            | Iterable[td_expr.IntoExprColumn]
            | bool
            | list[bool]
            | np.ndarray[Any, Any]
        ),
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Filter the `TableFrame` based on the given predicates.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.filter(td.col("a").is_in(["A", "C"]).or_(td.col("b").eq(10)))
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ A   ┆ 100 │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            self._lf.filter(
                *[
                    td_translator._unwrap_into_tdexpr_column(column)
                    for column in predicates
                ],
            )
        )

    # TODO: should we hide the named_exprs parameter?
    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def select(
        self,
        *exprs: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        **named_exprs: td_expr.IntoExpr,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Select column(s) from the `TableFrame`.

        Args:
            exprs: Columns or expressions to select.
            named_exprs: Named expressions to select.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.select(td.col("a"), td.col("b").mul(2).alias("bx2"),)
        >>>
        ┌─────┬─────┐
        │ a   ┆ bx2 │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 2   │
        │ X   ┆ 20  │
        │ C   ┆ 6   │
        │ D   ┆ 10  │
        │ M   ┆ 18  │
        │ A   ┆ 200 │
        │ M   ┆ 100 │
        └─────┴─────┘
        """
        source_columns = self._lf.collect_schema().names()
        # noinspection PyProtectedMember
        target_columns = (
            self._lf.select(
                *[td_translator._unwrap_into_tdexpr(column) for column in exprs],
                **named_exprs,
            )
            .collect_schema()
            .names()
        )
        # noinspection PyProtectedMember
        necessary_columns = [
            col
            for col in td_helpers.REQUIRED_COLUMNS
            if col in source_columns and col not in target_columns
        ]
        # noinspection PyProtectedMember
        columns = td_translator._args_to_tuple(
            *[td_translator._unwrap_into_tdexpr(column) for column in exprs],
            necessary_columns,
        )
        return TableFrame.__build__(
            _assemble_columns(
                self._lf.select(
                    columns,
                    **named_exprs,
                )
            )
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="aggregation")
    def group_by(
        self,
        *by: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
    ) -> td_group_by.TableFrameGroupBy:
        # noinspection PyShadowingNames
        """
        Perform a group by on the `TableFrame`.

        Args:
            by: Columns or expressions to group by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b").sum())
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ M   ┆ 59  │
        │ A   ┆ 101 │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return td_group_by.TableFrameGroupBy(
            self._lf.group_by(
                *[td_translator._unwrap_into_tdexpr(column) for column in by],
                maintain_order=False,
            )
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def slice(self, offset: int, length: int | None = None) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with a slice of the original `TableFrame`

        Args:
            offset: Slice starting index.
            length: The length of the slice. `None` means all the way to the end.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.slice(2,2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.slice(offset=offset, length=length))

    # status(Status.DONE)
    @pydoc(categories="filters")
    def limit(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first `n` rows.
        This is equivalent to `head`.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.limit(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.limit(n=n))

    # status(Status.DONE)
    @pydoc(categories="filters")
    def head(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first `n` rows.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.head(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.head(n=n))

    # status(Status.DONE)
    @pydoc(categories="filters")
    def tail(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the last `n` rows.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.tail(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.tail(n=n))

    # status(Status.DONE)
    @pydoc(categories="filters")
    def last(self) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the last row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.last()
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ M   ┆ 9   │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.last())

    # status(Status.DONE)
    @pydoc(categories="filters")
    def first(self) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.first()
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        └─────┴─────┘
        """
        return TableFrame.__build__(self._lf.first())

    """ Functions derived from DataFrame"""

    @unstable()
    @pydoc(categories="projection")
    def item(self) -> Any:
        # noinspection PyProtectedMember
        return td_frame.DataFrame.item(td_translator._unwrap_table_frame(self))


TdType = TypeVar("TdType", TableFrame, Series, td_expr.Expr)

"""Internal private Functions."""


def _assemble_columns(f: TableFrame | pl.LazyFrame) -> td.TableFrame:
    if isinstance(f, pl.LazyFrame):
        return td.TableFrame.__build__(Interceptor.instance().assemble_columns(f))
    elif isinstance(f, td.TableFrame):
        # noinspection PyProtectedMember
        return td.TableFrame.__build__(Interceptor.instance().assemble_columns(f._lf))
    else:
        raise TypeError(
            "Expected frame to be of type TableFrame or LazyFrame, but got"
            f" {type(f).__name__} instead."
        )


def get_class_methods(cls) -> List[str]:
    methods = [func for func in dir(cls) if callable(getattr(cls, func))]
    methods.sort()
    return methods


def get_missing_methods():
    polars_methods = get_class_methods(pl.LazyFrame)
    tabsdata_methods = get_class_methods(TableFrame)
    tabsdata_all_methods = set(
        tabsdata_methods
        + td_constants.DUPLICATE_METHODS
        + td_constants.FUNCTION_METHODS
        + td_constants.INTERNAL_METHODS
        + td_constants.MATERIALIZE_METHODS
        + td_constants.RENAME_METHODS
        + td_constants.UNNECESSARY_METHODS
        + td_constants.UNRECOMMENDED_METHODS
        + td_constants.UNSUPPORTED_METHODS
        + td_constants.UNSTABLE_METHODS
    )
    diff = list(set(polars_methods) - tabsdata_all_methods)
    # We determine if running inside a pytest test using the standard procedure:
    # https://docs.pytest.org/en/stable/example/simple.html#detect-if-running-from
    # -within-a-pytest-run
    if diff:
        if os.environ.get(td_constants.PYTEST_CONTEXT_ACTIVE) is not None:
            logger.warning(
                "🧨 There are some polars LazyDataFrame methods not available in"
                " TableFrame"
            )
            for polars_method in diff:
                logger.warning(f"   👀 {polars_method}")


def check_polars_api():
    """
    Check polars API.
    """
    logger.info("Available TableFrame methods:")
    for method in get_class_methods(TableFrame):
        logger.debug(f"   {method}")
    get_missing_methods()


# Check polars API changes the first time this module is loaded.
check_polars_api()
