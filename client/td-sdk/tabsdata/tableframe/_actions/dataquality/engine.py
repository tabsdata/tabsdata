#
# Copyright 2025 Tabs Data Inc.
#

from typing import TYPE_CHECKING, Any, Callable, Collection, Literal, Self

import polars as pl

import tabsdata as td

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator
import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.typing as td_typing

if TYPE_CHECKING:
    import tabsdata.tableframe.lazyframe.frame as td_frame

DEFAULT_POSTFIX = "_dq"


class DataQualityEngine:
    def __init__(self, tf: "td_frame.TableFrame"):
        self._tf = tf
        self._postfix: str | None = None
        self._data_columns = set(tf.schema.names())
        self._qdata_columns = set()
        self._name_indices: dict[tuple[str, str], int] = {}

    """> Special Functions """

    def __str__(self) -> str:
        return self._tf.__str__()

    def __repr__(self) -> str:
        return self.__str__()

    """> Internal Helper Functions """

    def _new(self, tf: "td_frame.TableFrame"):
        dq = DataQualityEngine(tf)
        dq._postfix = self._postfix
        dq._data_columns = self._data_columns.copy()
        dq._qdata_columns = self._qdata_columns.copy()
        dq._name_indices = self._name_indices.copy()
        return dq

    def _run(
        self,
        expression: td_expr.Expr,
        column: str,
    ) -> Self:
        return self._new(self._tf.with_columns(expression.alias(column)))

    def _name(
        self,
        data_column_name: str,
        qdata_column_name: str | None = None,
    ) -> str:
        if qdata_column_name is not None:
            base_name = qdata_column_name
            postfix = ""
        else:
            base_name = data_column_name
            postfix = self._postfix if self._postfix is not None else DEFAULT_POSTFIX
        index_key = (base_name, postfix)
        candidate_name = f"{base_name}{postfix}"
        if (
            candidate_name in self._data_columns
            or candidate_name in self._qdata_columns
        ):
            if index_key not in self._name_indices:
                self._name_indices[index_key] = 1
            index = self._name_indices[index_key]
            candidate_name = f"{base_name}{index}{postfix}"
            while (
                candidate_name in self._data_columns
                or candidate_name in self._qdata_columns
            ):
                index += 1
                candidate_name = f"{base_name}{index}{postfix}"
            self._name_indices[index_key] = index + 1
        self._qdata_columns.add(candidate_name)
        return candidate_name

    """> Attribute Functions """

    @property
    def postfix(self) -> str | None:
        return self._postfix

    def with_postfix(self, postfix: str | None = None) -> Self:
        self._postfix = postfix
        return self

    """> Context Functions """

    def tf(self):
        return self._tf

    """> Quality Specialized Functions """

    def is_null(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_null(),
            self._name(data_column_name, dq_column_name),
        )

    def is_not_null(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_not_null(),
            self._name(data_column_name, dq_column_name),
        )

    def is_nan(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_nan(),
            self._name(data_column_name, dq_column_name),
        )

    def is_not_nan(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_not_nan(),
            self._name(data_column_name, dq_column_name),
        )

    def is_null_or_nan(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_null() | pl.col(data_column_name).is_nan(),
            self._name(data_column_name, dq_column_name),
        )

    def is_not_null_or_nan(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_not_null()
            & pl.col(data_column_name).is_not_nan(),
            self._name(data_column_name, dq_column_name),
        )

    def is_in(
        self,
        data_column_name: str,
        data_column_values: Collection[Any],
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_in(data_column_values),
            self._name(data_column_name, dq_column_name),
        )

    def is_not_in(
        self,
        data_column_name: str,
        data_column_values: Collection[Any],
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            ~pl.col(data_column_name).is_in(data_column_values),
            self._name(data_column_name, dq_column_name),
        )

    def is_positive(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).gt(0),
            self._name(data_column_name, dq_column_name),
        )

    def is_positive_or_zero(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).ge(0),
            self._name(data_column_name, dq_column_name),
        )

    def is_negative(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).lt(0),
            self._name(data_column_name, dq_column_name),
        )

    def is_negative_or_zero(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).le(0),
            self._name(data_column_name, dq_column_name),
        )

    def is_zero(
        self,
        data_column_name: str,
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).eq(0),
            self._name(data_column_name, dq_column_name),
        )

    def is_between(
        self,
        data_column_name: str,
        lower_bound: int | float,
        upper_bound: int | float,
        closed: td_typing.ClosedInterval = "both",
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            pl.col(data_column_name).is_between(lower_bound, upper_bound, closed),
            self._name(data_column_name, dq_column_name),
        )

    def is_not_between(
        self,
        data_column_name: str,
        lower_bound: int | float,
        upper_bound: int | float,
        closed: td_typing.ClosedInterval = "both",
        dq_column_name: str | None = None,
    ) -> Self:
        return self._run(
            ~pl.col(data_column_name).is_between(lower_bound, upper_bound, closed),
            self._name(data_column_name, dq_column_name),
        )

    """> Quality Generic Functions """

    def expr(
        self,
        expr: td_expr.Expr,
        dq_column_name: str,
    ) -> Self:
        pl_expr = td_translator._unwrap_into_tdexpr_column(expr)
        pl_schema = self._tf._lf.select(pl_expr).collect_schema()
        if pl_schema.len() != 1:
            raise ValueError(
                "Expression must resolve to exactly one column, but it resolved to "
                f"{pl_schema.len()} columns: {list(pl_schema.keys())}"
            )
        return self._run(
            expr,
            dq_column_name,
        )

    def fn(
        self,
        data_column_names: str | list[str],
        dq_column_dtype: type["td.Int8"] | type["td.Boolean"],
        fn: Callable[..., Any] | Callable[..., pl.Series],
        fn_mode: Literal["row", "batch"],
        dq_column_name: str | None = None,
    ) -> Self:
        if isinstance(data_column_names, str):
            data_column_names = [data_column_names]
        if fn_mode == "batch":

            def map_fn(batch: pl.Series) -> pl.Series:
                return fn(*[batch.struct.field(column) for column in data_column_names])

            expr = pl.struct(data_column_names).map_batches(
                map_fn,
                dq_column_dtype,
            )
        elif fn_mode == "row":

            def map_fn(batch: pl.Series) -> pl.Series:
                series_i = [batch.struct.field(column) for column in data_column_names]
                series_o = []
                for series in zip(*series_i):
                    series_o.append(fn(*series))
                return pl.Series(series_o, dtype=dq_column_dtype)

            expr = pl.struct(data_column_names).map_batches(
                map_fn,
                dq_column_dtype,
            )
        else:
            raise ValueError(f"Invalid fn_mode: {fn_mode}. Must be 'row' or 'batch'.")

        return self._run(expr, dq_column_name)
