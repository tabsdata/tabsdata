#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Callable, Self, TypeVar

import polars as pl

import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.frame as td_frame
from tabsdata.dataquality import IsNotZero, IsZero

# noinspection PyProtectedMember
from tabsdata.dataquality._classifier import (
    Classifier,
    IsNan,
    IsNotNan,
    IsNotNull,
    IsNotNullNorNan,
    IsNull,
    IsNullOrNan,
)
from tabsdata.exceptions import DataQualityProcessingError, ErrorCode
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdata.dataquality._typing import ColumnNameWithAltSpec, TagsSpec

DQ_VERSION = 1

logger = logging.getLogger(__name__)

ClassifierType = TypeVar("ClassifierType", bound=Classifier)


class DQColumns(Enum):
    DQ_VERSION = "dq.version"
    DQ_CLASSIFIER = "dq.classifier"
    DQ_TYPE = "dq.type"
    DQ_BUCKETS = "dq.buckets"
    DQ_TRUE = "dq.true"
    DQ_FALSE = "dq.false"
    DQ_CATEGORY = "dq.p{:02d}"
    DQ_PARAMETERS = "dq.parameters"

    @classmethod
    def column(cls, index: int) -> str:
        return cls.DQ_CATEGORY.value.format(index)


class ClassifierFamily(Enum):
    BOOLEAN = "boolean"
    CATEGORICAL = "categorical"


class ByteSentinel(IntEnum):
    FALSE = 0
    TRUE = 1
    NAN = 254
    NULL = 255


class QCallableEnum(Enum):
    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class QExpressions(QCallableEnum):
    IS_NAN = lambda column, dtype: (  # noqa: E731
        column.is_nan() if dtype in [pl.Float32, pl.Float64] else pl.lit(False)
    )


class PolarsProvider:
    def __init__(
        self, function: Callable[[pl.Expr, pl.DataType, ClassifierType], pl.Expr]
    ):
        self._function = function
        signature = inspect.signature(function)
        self._needs_classifier = len(signature.parameters) > 2

    def __call__(
        self, column: str, dtype: pl.DataType, classifier: ClassifierType
    ) -> pl.Expr:
        column_expr = pl.col(column)
        return self._function(column_expr, dtype, classifier)

    @staticmethod
    def is_null(column: pl.Expr, dtype: pl.DataType, _: IsNull) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.TRUE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(ByteSentinel.FALSE)
        )

    @staticmethod
    def is_not_null(column: pl.Expr, dtype: pl.DataType, _: IsNotNull) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.FALSE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(ByteSentinel.TRUE)
        )

    @staticmethod
    def is_nan(column: pl.Expr, dtype: pl.DataType, _: IsNan) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.TRUE)
            .otherwise(ByteSentinel.FALSE)
        )

    @staticmethod
    def is_not_nan(column: pl.Expr, dtype: pl.DataType, _: IsNotNan) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.FALSE)
            .otherwise(ByteSentinel.TRUE)
        )

    @staticmethod
    def is_null_or_nan(column: pl.Expr, dtype: pl.DataType, _: IsNullOrNan) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.TRUE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.TRUE)
            .otherwise(ByteSentinel.FALSE)
        )

    @staticmethod
    def is_not_null_nor_nan(
        column: pl.Expr, dtype: pl.DataType, _: IsNotNullNorNan
    ) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.FALSE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.FALSE)
            .otherwise(ByteSentinel.TRUE)
        )

    @staticmethod
    def is_zero(column: pl.Expr, dtype: pl.DataType, _: IsZero) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(column.eq(0))
        )

    @staticmethod
    def is_not_zero(column: pl.Expr, dtype: pl.DataType, _: IsNotZero) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(column.ne(0))
        )


@dataclass(frozen=True)
class ClassifierMetadata:
    column_suffix: str
    classifier_family: ClassifierFamily
    polars_provider: PolarsProvider


MappedColumn = namedtuple("MappedColumn", ["data_column", "dq_column"])


class DataQualityEngine:
    def __init__(
        self,
        tf: td_frame.TableFrame,
        cols: dict[str, ClassifierFamily] = None,
        tags: dict[str, set[str]] = None,
    ):
        self._tf: td_frame.TableFrame = tf
        self._cols: dict[str, ClassifierFamily] = cols or {}
        self._tags: dict[str, set[str]] = tags or {}

    """> Special Functions """

    def __str__(self) -> str:
        return self._tf.__str__()

    def __repr__(self) -> str:
        return self.__str__()

    """> Internal Helper Functions """

    @classmethod
    def _cols_spec_to_cols_list(
        cls, cols_spec: list[ColumnNameWithAltSpec]
    ) -> list[MappedColumn]:
        match cols_spec:
            case None:
                return []
            case str():
                return [MappedColumn(data_column=cols_spec, dq_column=None)]
            case tuple():
                return [MappedColumn(data_column=cols_spec[0], dq_column=cols_spec[1])]
            case list():
                cols_list = []
                for col_spec in cols_spec:
                    if isinstance(col_spec, str):
                        cols_list.append(
                            MappedColumn(data_column=col_spec, dq_column=None)
                        )
                    elif isinstance(col_spec, tuple):
                        cols_list.append(
                            MappedColumn(data_column=col_spec[0], dq_column=col_spec[1])
                        )
                    else:
                        raise TypeError(f"Invalid columns specification: {cols_spec}")
                return cols_list
            case _:
                raise TypeError(f"Invalid columns specification: {cols_spec}")

    @classmethod
    def _tags_spec_to_list(cls, tags_spec: TagsSpec) -> list[str]:
        match tags_spec:
            case None:
                return []
            case str():
                return [tags_spec]
            case list():
                return tags_spec
            case _:
                raise TypeError(f"Invalid tags specification: {tags_spec}")

    def _columns(self, tags: set[str] | None) -> set[str]:
        vtags = tags if tags else self._tags.keys()
        return set().union(*(self._tags[tag] for tag in vtags if tag in self._tags))

    @classmethod
    def _name(
        cls,
        classifier: str,
        lf_column: str,
        dq_column: str | None = None,
    ) -> str:
        return dq_column if dq_column is not None else f"{lf_column}{classifier}"

    def _new(self, tf: td_frame.TableFrame):
        dq = DataQualityEngine(tf)
        dq._cols = self._cols
        dq._tags = self._tags
        return dq

    def _run(
        self,
        expression: td_expr.Expr,
        column: str,
    ) -> Self:
        return self._new(self._tf.with_columns(expression.alias(column)))

    """> Context Functions """

    def tf(self):
        return self._tf

    """> Quality Classification Functions """

    def _classify(
        self,
        classifier: Classifier,
    ) -> Self:
        classifier_type = type(classifier)

        metadata = CLASSIFIER_DICTIONARY.get(classifier_type)

        if metadata is None:
            # noinspection PyProtectedMember
            raise DataQualityProcessingError(
                ErrorCode.DQPE9,
                classifier_type.__name__,
                self._tf._data_table.name,
            )

        columns = self._cols_spec_to_cols_list(classifier.column_names)
        tags = self._tags_spec_to_list(classifier.tags)
        dq = self

        for column in columns:
            lf_column = column.data_column
            dq_column = self._name(metadata.column_suffix, lf_column, column.dq_column)
            self._cols.update({dq_column: metadata.classifier_family})
            vtags = tags if tags else [None]
            for tag in vtags:
                if tag not in self._tags:
                    self._tags[tag] = set()
                self._tags[tag].add(dq_column)
            lf_dtype = self._tf._lf.schema[lf_column]
            expr = metadata.polars_provider(lf_column, lf_dtype, classifier)
            dq = dq._run(expr, dq_column)

        return dq

    def is_null(
        self,
        is_null: IsNull,
    ) -> Self:
        return self._classify(is_null)

    def is_not_null(
        self,
        is_not_null: IsNotNull,
    ) -> Self:
        return self._classify(is_not_null)

    def is_nan(
        self,
        is_nan: IsNan,
    ) -> Self:
        return self._classify(is_nan)

    def is_not_nan(
        self,
        is_not_nan: IsNotNan,
    ) -> Self:
        return self._classify(is_not_nan)

    def is_zero(
        self,
        is_zero: IsZero,
    ) -> Self:
        return self._classify(is_zero)

    def is_not_zero(
        self,
        is_not_zero: IsNotZero,
    ) -> Self:
        return self._classify(is_not_zero)

    """> Quality Operation Functions """

    def summary(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = set(self._tags_spec_to_list(tags_spec))
        columns = self._columns(tags)

        dq_columns = []
        for column in columns:
            if column not in self._cols:
                raise DataQualityProcessingError(
                    ErrorCode.DQPE10,
                    column,
                )
            if self._cols[column] == ClassifierFamily.BOOLEAN:
                dq_columns.append(column)

        # noinspection PyProtectedMember
        lf = self._tf._lf.select(dq_columns)
        summary_data = []
        for column in sorted(dq_columns):
            true_count = lf.select(pl.col(column).sum()).collect().item()
            false_count = lf.select((~pl.col(column)).sum()).collect().item()
            summary_row = {
                DQColumns.DQ_VERSION.value: DQ_VERSION,
                DQColumns.DQ_CLASSIFIER.value: column,
                DQColumns.DQ_TYPE.value: ClassifierFamily.BOOLEAN.value,
                DQColumns.DQ_BUCKETS.value: None,
                DQColumns.DQ_TRUE.value: true_count,
                DQColumns.DQ_FALSE.value: false_count,
            }
            for i in range(100):
                summary_row[DQColumns.column(i)] = None
            summary_row[DQColumns.DQ_PARAMETERS.value] = None
            summary_data.append(summary_row)

        # Parquet format doesn't natively support 128-bit integer types.
        # We stick to 64-bit integers for now.
        schema = {
            DQColumns.DQ_VERSION.value: pl.UInt8,
            DQColumns.DQ_CLASSIFIER.value: pl.String,
            DQColumns.DQ_TYPE.value: pl.String,
            DQColumns.DQ_BUCKETS.value: pl.UInt8,
            DQColumns.DQ_TRUE.value: pl.UInt64,
            DQColumns.DQ_FALSE.value: pl.UInt64,
        }
        for i in range(100):
            schema[DQColumns.column(i)] = pl.UInt64
        schema[DQColumns.DQ_PARAMETERS.value] = pl.String

        summary_lf = pl.DataFrame(summary_data, schema=schema).lazy()

        summary_tf = td_frame.TableFrame.__build__(
            df=summary_lf,
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )

        return DataQualityEngine(
            tf=summary_tf,
            tags=self._tags,
        )

    # ToDo: Work in progress...
    # noinspection DuplicatedCode
    def filter(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = set(self._tags_spec_to_list(tags_spec))
        quality_columns_for_tags = self._columns(tags)

        # ToDo...
        columns_to_select = list(quality_columns_for_tags)

        filtered_tf = self._tf.select(columns_to_select)

        return DataQualityEngine(
            tf=filtered_tf,
            tags=self._tags,
        )

    # ToDo: Work in progress...
    # noinspection DuplicatedCode
    def select(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = set(self._tags_spec_to_list(tags_spec))
        quality_columns_for_tags = self._columns(tags)

        # ToDo...
        columns_to_select = list(quality_columns_for_tags)

        filtered_tf = self._tf.select(columns_to_select)

        return DataQualityEngine(
            tf=filtered_tf,
            tags=self._tags,
        )

    # ToDo: Work in progress...

    # def is_nan(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_nan(),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_not_nan(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_not_nan(),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_null_or_nan(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_null() | pl.col(data_column_name).is_nan(),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_not_null_or_nan(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_not_null()
    #         & pl.col(data_column_name).is_not_nan(),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_in(
    #     self,
    #     data_column_name: str,
    #     data_column_values: Collection[Any],
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_in(data_column_values),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_not_in(
    #     self,
    #     data_column_name: str,
    #     data_column_values: Collection[Any],
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         ~pl.col(data_column_name).is_in(data_column_values),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_positive(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).gt(0),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_positive_or_zero(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).ge(0),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_negative(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).lt(0),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_negative_or_zero(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).le(0),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_zero(
    #     self,
    #     data_column_name: str,
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).eq(0),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_between(
    #     self,
    #     data_column_name: str,
    #     lower_bound: int | float,
    #     upper_bound: int | float,
    #     closed: td_typing.ClosedInterval = "both",
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         pl.col(data_column_name).is_between(lower_bound, upper_bound, closed),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # def is_not_between(
    #     self,
    #     data_column_name: str,
    #     lower_bound: int | float,
    #     upper_bound: int | float,
    #     closed: td_typing.ClosedInterval = "both",
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     return self._run(
    #         ~pl.col(data_column_name).is_between(lower_bound, upper_bound, closed),
    #         self._name(data_column_name, dq_column_name),
    #     )
    #
    # """> Quality Generic Functions """
    #
    # def expr(
    #     self,
    #     expr: td_expr.Expr,
    #     dq_column_name: str,
    # ) -> Self:
    #     pl_expr = td_translator._unwrap_into_tdexpr_column(expr)
    #     pl_schema = self._tf._lf.select(pl_expr).collect_schema()
    #     if pl_schema.len() != 1:
    #         raise ValueError(
    #             "Expression must resolve to exactly one column, but it resolved to "
    #             f"{pl_schema.len()} columns: {list(pl_schema.keys())}"
    #         )
    #     return self._run(
    #         expr,
    #         dq_column_name,
    #     )
    #
    # def fn(
    #     self,
    #     data_column_names: str | list[str],
    #     dq_column_dtype: type["td.Int8"] | type["td.Boolean"],
    #     fn: Callable[..., Any] | Callable[..., pl.Series],
    #     fn_mode: Literal["row", "batch"],
    #     dq_column_name: str | None = None,
    # ) -> Self:
    #     if isinstance(data_column_names, str):
    #         data_column_names = [data_column_names]
    #     if fn_mode == "batch":
    #
    #         def map_fn(batch: pl.Series) -> pl.Series:
    #             return fn(*[batch.struct.field(column)
    #             for column in data_column_names])
    #
    #         expr = pl.struct(data_column_names).map_batches(
    #             map_fn,
    #             dq_column_dtype,
    #         )
    #     elif fn_mode == "row":
    #
    #         def map_fn(batch: pl.Series) -> pl.Series:
    #             series_i = [batch.struct.field(column)
    #             for column in data_column_names]
    #             series_o = []
    #             for series in zip(*series_i):
    #                 series_o.append(fn(*series))
    #             return pl.Series(series_o, dtype=dq_column_dtype)
    #
    #         expr = pl.struct(data_column_names).map_batches(
    #             map_fn,
    #             dq_column_dtype,
    #         )
    #     else:
    #         raise ValueError(f"Invalid fn_mode: {fn_mode}. Must be 'row' or 'batch'.")
    #
    #     return self._run(expr, dq_column_name)


CLASSIFIER_DICTIONARY: dict[type, ClassifierMetadata] = {
    IsNull: ClassifierMetadata(
        column_suffix="_is_null",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_null),
    ),
    IsNotNull: ClassifierMetadata(
        column_suffix="_is_not_null",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_null),
    ),
    IsNan: ClassifierMetadata(
        column_suffix="_is_nan",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_nan),
    ),
    IsNotNan: ClassifierMetadata(
        column_suffix="_is_not_nan",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_nan),
    ),
    IsNullOrNan: ClassifierMetadata(
        column_suffix="_is_null_or_nan",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_null_or_nan),
    ),
    IsNotNullNorNan: ClassifierMetadata(
        column_suffix="_is_not_null_nor_nan",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_null_nor_nan),
    ),
    IsZero: ClassifierMetadata(
        column_suffix="_is_zero",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_zero),
    ),
    IsNotZero: ClassifierMetadata(
        column_suffix="_is_not_zero",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_zero),
    ),
    # IsPositive: ClassifierMetadata(
    #     suffix="_is_positive",
    #     family=ClassifierFamily.BOOLEAN,
    #     polars=lambda col: col.gt(0),
    # ),
    # IsPositiveOrZero: ClassifierMetadata(
    #     suffix="_is_positive_or_zero",
    #     family=ClassifierFamily.BOOLEAN,
    #     polars=lambda col: col.ge(0),
    # ),
    # IsNegative: ClassifierMetadata(
    #     suffix="_is_negative",
    #     family=ClassifierFamily.BOOLEAN,
    #     polars=lambda col: col.lt(0),
    # ),
    # IsNegativeOrZero: ClassifierMetadata(
    #     suffix="_is_negative_or_zero",
    #     family=ClassifierFamily.BOOLEAN,
    #     polars=lambda col: col.le(0),
    # ),
}
