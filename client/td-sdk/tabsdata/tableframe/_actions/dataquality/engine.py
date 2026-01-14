#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
import math
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, AbstractSet, Any, Callable, Self, TypeVar, cast

import polars as pl
from polars._typing import ClosedInterval

import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing
from tabsdata.dataquality import (
    DoesNotMatch,
    HasLength,
    IsBetween,
    IsIn,
    IsNotBetween,
    IsNotIn,
    IsNotZero,
    IsPositive,
    IsPositiveOrZero,
    IsZero,
    Matches,
)

# noinspection PyProtectedMember
from tabsdata.dataquality._classifier import (
    Categorizer,
    Classifier,
    ExponentialScale,
    IdentityScale,
    IsNan,
    IsNegative,
    IsNegativeOrZero,
    IsNotNan,
    IsNotNull,
    IsNotNullNorNan,
    IsNull,
    IsNullOrNan,
    LinearScale,
    LogarithmicScale,
    MonomialScale,
)
from tabsdata.exceptions import DataQualityProcessingError, ErrorCode
from tabsdata.tableframe._actions.dataquality.assistant import TypeHandler
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdata.dataquality._typing import ColumnNameWithAltSpec, TagsSpec

DQ_VERSION = 1
MAX_BINS = 100

PolarsClosedInterval = ClosedInterval
RangeClassifier = IsBetween | IsNotBetween

CLOSED_MAPPING: dict[str, PolarsClosedInterval] = {
    "lower": "left",
    "upper": "right",
    "both": "both",
    "none": "none",
}

logger = logging.getLogger(__name__)

ClassifierType = TypeVar("ClassifierType", bound=Classifier)


class DQColumns(Enum):
    DQ_VERSION = "dq.version"
    DQ_CLASSIFIER = "dq.classifier"
    DQ_TYPE = "dq.type"
    DQ_BINS = "dq.bins"
    DQ_FALSE = "dq.false"
    DQ_TRUE = "dq.true"
    DQ_CATEGORY = "dq.p{:03d}"
    DQ_UNDERFLOW = "dq.under"
    DQ_OVERFLOW = "dq.over"
    DQ_NAN = "dq.nan"
    DQ_NONE = "dq.none"
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
    UNDERFLOW = 252
    OVERFLOW = 253
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
        self,
        function: Callable[
            [
                pl.Expr,
                pl.DataType,
                ClassifierType,
            ],
            pl.Expr,
        ],
    ):
        self._function = function
        signature = inspect.signature(function)
        self._needs_classifier = len(signature.parameters) > 2

    def __call__(
        self,
        column: str,
        dtype: pl.DataType,
        classifier: ClassifierType,
    ) -> pl.Expr:
        column_expr = pl.col(column)
        return self._function(column_expr, dtype, classifier)

    @staticmethod
    def _null(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.is_null()

    @staticmethod
    def is_null(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNull,
    ) -> pl.Expr:
        return (
            pl.when(PolarsProvider._null(column))
            .then(ByteSentinel.TRUE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(ByteSentinel.FALSE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def is_not_null(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNotNull,
    ) -> pl.Expr:
        return (
            pl.when(PolarsProvider._null(column))
            .then(ByteSentinel.FALSE)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .otherwise(ByteSentinel.TRUE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def _nan(
        column: pl.Expr,
        dtype: pl.DataType,
    ) -> pl.Expr:
        return QExpressions.IS_NAN(column, dtype)

    @staticmethod
    def is_nan(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNan,
    ) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(PolarsProvider._nan(column, dtype))
            .then(ByteSentinel.TRUE)
            .otherwise(ByteSentinel.FALSE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def is_not_nan(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNotNan,
    ) -> pl.Expr:
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(PolarsProvider._nan(column, dtype))
            .then(ByteSentinel.FALSE)
            .otherwise(ByteSentinel.TRUE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def _null_or_nan(
        column: pl.Expr,
        dtype: pl.DataType,
    ) -> pl.Expr:
        return column.is_null() | QExpressions.IS_NAN(column, dtype)

    @staticmethod
    def is_null_or_nan(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNullOrNan,
    ) -> pl.Expr:
        return (
            pl.when(PolarsProvider._null_or_nan(column, dtype))
            .then(ByteSentinel.TRUE)
            .otherwise(ByteSentinel.FALSE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def is_not_null_nor_nan(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNotNullNorNan,
    ) -> pl.Expr:
        return (
            pl.when(PolarsProvider._null_or_nan(column, dtype))
            .then(ByteSentinel.FALSE)
            .otherwise(ByteSentinel.TRUE)
            .cast(pl.UInt8)
        )

    @staticmethod
    def _zero(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.eq(0)

    @staticmethod
    def is_zero(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsZero,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._zero(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def is_not_zero(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNotZero,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = ~PolarsProvider._zero(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _positive(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.gt(0)

    @staticmethod
    def is_positive(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsPositive,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._positive(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _positive_or_zero(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.ge(0)

    @staticmethod
    def is_positive_or_zero(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsPositiveOrZero,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._positive_or_zero(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _negative(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.lt(0)

    @staticmethod
    def is_negative(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNegative,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._negative(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _negative_or_zero(
        column: pl.Expr,
    ) -> pl.Expr:
        return column.le(0)

    @staticmethod
    def is_negative_or_zero(
        column: pl.Expr,
        dtype: pl.DataType,
        _: IsNegativeOrZero,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._negative_or_zero(column)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _between(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: RangeClassifier,
    ) -> pl.Expr:
        c_min = TypeHandler.convert_value(classifier.min, dtype, allow_null=True)
        c_max = TypeHandler.convert_value(classifier.max, dtype, allow_null=True)
        if c_min is None and c_max is None:
            return pl.lit(True)
        if c_min is None:
            if classifier.closed in ("both", "upper"):
                return column.le(pl.lit(c_max))
            return column.lt(pl.lit(c_max))
        if c_max is None:
            if classifier.closed in ("both", "lower"):
                return column.ge(pl.lit(c_min))
            return column.gt(pl.lit(c_min))
        return column.is_between(
            pl.lit(c_min),
            pl.lit(c_max),
            closed=CLOSED_MAPPING[classifier.closed],
        )

    @staticmethod
    def is_between(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: IsBetween,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._between(column, dtype, classifier)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def is_not_between(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: IsNotBetween,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = ~PolarsProvider._between(column, dtype, classifier)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _in(
        column: pl.Expr,
        dtype: pl.DataType,
        values: AbstractSet[Any],
    ) -> pl.Expr:
        c_values = TypeHandler.convert_set(values, dtype)
        return column.is_in(c_values)

    # noinspection DuplicatedCode
    @staticmethod
    def is_in(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: IsIn,
    ) -> pl.Expr:
        has_none = None in classifier.values
        has_nan = any(
            isinstance(value, float) and math.isnan(value)
            for value in classifier.values
        )
        if has_none and has_nan:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.TRUE)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.TRUE)
            )
        elif has_none:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.TRUE)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.NAN)
            )
        elif has_nan:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.NULL)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.TRUE)
            )
        else:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.NULL)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.NAN)
            )
        core_expression = PolarsProvider._in(column, dtype, classifier.values)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    # noinspection DuplicatedCode
    @staticmethod
    def is_not_in(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: IsNotIn,
    ) -> pl.Expr:
        has_none = None in classifier.values
        has_nan = any(
            isinstance(value, float) and math.isnan(value)
            for value in classifier.values
        )

        if has_none and has_nan:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.FALSE)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.FALSE)
            )
        elif has_none:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.FALSE)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.NAN)
            )
        elif has_nan:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.NULL)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.FALSE)
            )
        else:
            base_expression = (
                pl.when(column.is_null())
                .then(ByteSentinel.NULL)
                .when(QExpressions.IS_NAN(column, dtype))
                .then(ByteSentinel.NAN)
            )
        core_expression = ~PolarsProvider._in(column, dtype, classifier.values)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _matches(
        column: pl.Expr,
        pattern: str,
    ) -> pl.Expr:
        return column.cast(pl.Utf8).str.contains(pattern)

    @staticmethod
    def matches(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: Matches,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._matches(column, classifier.pattern)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def does_not_match(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: DoesNotMatch,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = ~PolarsProvider._matches(column, classifier.pattern)
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def _has_length(
        column: pl.Expr,
        min_len: int,
        max_len: int,
    ) -> pl.Expr:
        return (
            column.cast(pl.Utf8)
            .str.len_chars()
            .is_between(pl.lit(min_len), pl.lit(max_len), closed="both")
        )

    @staticmethod
    def has_length(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: HasLength,
    ) -> pl.Expr:
        base_expression = (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
        )
        core_expression = PolarsProvider._has_length(
            column, classifier.min, classifier.max
        )
        return base_expression.otherwise(core_expression).cast(pl.UInt8)

    @staticmethod
    def categorizer(
        column: pl.Expr,
        dtype: pl.DataType,
        classifier: Categorizer,
    ) -> pl.Expr:
        scale = classifier.scale
        minimum, maximum = scale.scale_range

        cuts = []

        match scale:
            case IdentityScale():
                cuts = [minimum + i + 1 for i in range(scale.bins)]
            case LinearScale():
                size = (maximum - minimum) / scale.bins
                cuts = [minimum + (i + 1) * size for i in range(scale.bins)]
                cuts[-1] = maximum
            case MonomialScale():
                lower = minimum ** (1.0 / scale.power)
                upper = maximum ** (1.0 / scale.power)
                size = (upper - lower) / scale.bins
                cuts = [
                    (lower + (i + 1) * size) ** scale.power for i in range(scale.bins)
                ]
                cuts[-1] = maximum
            case LogarithmicScale():
                lower = scale.base**minimum
                upper = scale.base**maximum
                size = (upper - lower) / scale.bins
                if scale.base == 10.0:
                    cuts = [
                        math.log10(lower + (i + 1) * size) for i in range(scale.bins)
                    ]
                elif scale.base == math.e:
                    cuts = [math.log(lower + (i + 1) * size) for i in range(scale.bins)]
                else:
                    cuts = [
                        math.log(lower + (i + 1) * size) / math.log(scale.base)
                        for i in range(scale.bins)
                    ]
                cuts[-1] = maximum
            case ExponentialScale():
                if scale.base == 10.0:
                    lower = math.log10(minimum)
                    upper = math.log10(maximum)
                elif scale.base == math.e:
                    lower = math.log(minimum)
                    upper = math.log(maximum)
                else:
                    lower = math.log(minimum) / math.log(scale.base)
                    upper = math.log(maximum) / math.log(scale.base)
                size = (upper - lower) / scale.bins
                cuts = [
                    scale.base ** (lower + (i + 1) * size) for i in range(scale.bins)
                ]
                cuts[-1] = maximum
            case _:
                raise ValueError(f"Unsupported scale type: {type(scale)}")

        expression = None
        if not scale.min_in_first_bin:
            expression = pl.when(column == minimum).then(pl.lit(0))
        for i, cut in enumerate(cuts, start=1):
            if expression is None:
                expression = pl.when(column <= cut).then(pl.lit(i))
            else:
                expression = expression.when(column <= cut).then(pl.lit(i))
        return (
            pl.when(column.is_null())
            .then(ByteSentinel.NULL)
            .when(QExpressions.IS_NAN(column, dtype))
            .then(ByteSentinel.NAN)
            .when(column < minimum)
            .then(ByteSentinel.UNDERFLOW)
            .when(column > maximum)
            .then(ByteSentinel.OVERFLOW)
            .otherwise(expression)
            .cast(pl.UInt8)
        )


@dataclass(frozen=True)
class ClassifierMetadata:
    column_suffix: str
    classifier_family: ClassifierFamily
    polars_provider: PolarsProvider


@dataclass
class ColumnInfo:
    family: ClassifierFamily
    classifier: Classifier


MappedColumn = namedtuple("MappedColumn", ["data_column", "dq_column"])


class DataQualityEngine:
    def __init__(self, tf: td_frame.TableFrame):
        self._tf: td_frame.TableFrame = tf
        self._schema: td_typing.Schema = tf.schema
        self._cols: dict[str, ColumnInfo] = {}
        self._tags: dict[str, set[str]] = {}

    @classmethod
    def _new(
        cls,
        tf: td_frame.TableFrame,
        dq_i: DataQualityEngine = None,
        schema: td_typing.Schema = None,
        cols: dict[str, ColumnInfo] = None,
        tags: dict[str, set[str]] = None,
    ) -> Self:
        dq_o: DataQualityEngine = cls.__new__(cls)
        if dq_i is None:
            dq_o._tf = tf
            dq_o._schema = schema
            dq_o._cols = cols if cols is not None else {}
            dq_o._tags = tags if tags is not None else {}
        else:
            dq_o._tf = tf if tf is not None else dq_i._tf
            dq_o._schema = schema if schema is not None else dq_i._schema
            dq_o._cols = dq_i._cols if cols is None else cols
            dq_o._tags = dq_i._tags if tags is None else tags
        return dq_o

    """> Special Functions """

    def __str__(self) -> str:
        return self._tf.__str__()

    def __repr__(self) -> str:
        return self.__str__()

    """> Internal Helper Functions """

    def _cols_spec_to_cols_list(
        self,
        classifier: Classifier,
        cols_spec: list[ColumnNameWithAltSpec],
    ) -> list[MappedColumn]:
        schema = self._schema
        user_columns, system_columns = td_frame._split_columns(schema.names())
        supported_dtypes = classifier.supported_dtypes()

        columns = []
        match cols_spec:
            case None:
                columns = [(column, None) for column in sorted(user_columns)]
            case str():
                columns = [(cols_spec, None)]
            case tuple():
                columns = [(cols_spec[0], cols_spec[1])]
            case list():
                columns = []
                for col_spec in cols_spec:
                    if isinstance(col_spec, str):
                        columns.append((col_spec, None))
                    elif isinstance(col_spec, tuple):
                        columns.append((col_spec[0], col_spec[1]))
                    else:
                        raise TypeError(f"Invalid columns specification: {cols_spec}")
            case _:
                raise TypeError(f"Invalid columns specification: {cols_spec}")

        mapped_columns = []
        for data_column, dq_column in columns:
            if data_column not in user_columns:
                match classifier.on_missing_column:
                    case "fail":
                        raise DataQualityProcessingError(
                            ErrorCode.DQPE11,
                            data_column,
                            self._tf._data_table.name,
                        )
                    case "ignore":
                        continue
                    case _:
                        raise ValueError(
                            "Unknown on_missing_column value: "
                            f"{classifier.on_missing_column}"
                        )

            dtype = self._schema[data_column]
            if type(dtype) not in supported_dtypes:
                match classifier.on_wrong_type:
                    case "fail":
                        raise DataQualityProcessingError(
                            ErrorCode.DQPE12,
                            data_column,
                            type(dtype).__name__,
                            classifier.__class__.__name__,
                        )
                    case "ignore":
                        continue
                    case _:
                        raise ValueError(
                            f"Unknown on_wrong_type value: {classifier.on_wrong_type}"
                        )

            mapped_columns.append(
                MappedColumn(data_column=data_column, dq_column=dq_column)
            )

        return mapped_columns

    @classmethod
    def _tags_spec_to_list(cls, tags_spec: TagsSpec) -> list[str] | None:
        match tags_spec:
            case None:
                return None
            case str():
                return [tags_spec]
            case list():
                return tags_spec
            case _:
                raise TypeError(f"Invalid tags specification: {tags_spec}")

    def _columns(self, tags: set[str] | None) -> set[str]:
        if tags is None:
            vtags = self._tags.keys()
        elif len(tags) == 0:
            vtags = [None]
        else:
            vtags = tags
        return set().union(*(self._tags[tag] for tag in vtags if tag in self._tags))

    @classmethod
    def _name(
        cls,
        classifier: str,
        lf_column: str,
        dq_column: str | None = None,
    ) -> str:
        return dq_column if dq_column is not None else f"{lf_column}{classifier}"

    def _run(
        self,
        expression: td_expr.Expr,
        column: str,
    ) -> Self:
        return DataQualityEngine._new(
            tf=self._tf.with_columns(expression.alias(column)),
            dq_i=self,
            schema=None,
            cols=None,
            tags=None,
        )

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

        columns = self._cols_spec_to_cols_list(classifier, classifier.column_names)
        tags = self._tags_spec_to_list(classifier.tags)
        dq = self

        for column in columns:
            lf_column = column.data_column
            dq_column = self._name(metadata.column_suffix, lf_column, column.dq_column)
            self._cols.update(
                {
                    dq_column: ColumnInfo(
                        family=metadata.classifier_family,
                        classifier=classifier,
                    )
                }
            )
            vtags = tags if tags else [None]
            for tag in vtags:
                if tag not in self._tags:
                    self._tags[tag] = set()
                self._tags[tag].add(dq_column)
            lf_dtype = self._schema[lf_column]
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

    def is_null_or_nan(
        self,
        is_null_or_nan: IsNullOrNan,
    ) -> Self:
        return self._classify(is_null_or_nan)

    def is_not_null_nor_nan(
        self,
        is_not_null_nor_nan: IsNotNullNorNan,
    ) -> Self:
        return self._classify(is_not_null_nor_nan)

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

    def is_positive(
        self,
        is_positive: IsPositive,
    ) -> Self:
        return self._classify(is_positive)

    def is_positive_or_zero(
        self,
        is_positive_or_zero: IsPositiveOrZero,
    ) -> Self:
        return self._classify(is_positive_or_zero)

    def is_negative(
        self,
        is_negative: IsNegative,
    ) -> Self:
        return self._classify(is_negative)

    def is_negative_or_zero(
        self,
        is_negative_or_zero: IsNegativeOrZero,
    ) -> Self:
        return self._classify(is_negative_or_zero)

    def is_between(
        self,
        is_between: IsBetween,
    ) -> Self:
        return self._classify(is_between)

    def is_not_between(
        self,
        is_not_between: IsNotBetween,
    ) -> Self:
        return self._classify(is_not_between)

    def is_in(
        self,
        is_in: IsIn,
    ) -> Self:
        return self._classify(is_in)

    def is_not_in(
        self,
        is_not_in: IsNotIn,
    ) -> Self:
        return self._classify(is_not_in)

    def matches(
        self,
        matches: Matches,
    ) -> Self:
        return self._classify(matches)

    def does_not_match(
        self,
        does_not_match: DoesNotMatch,
    ) -> Self:
        return self._classify(does_not_match)

    def has_length(
        self,
        has_length: HasLength,
    ) -> Self:
        return self._classify(has_length)

    def categorizer(
        self,
        categorizer: Categorizer,
    ) -> Self:
        return self._classify(categorizer)

    """> Quality Operation Functions """

    # noinspection DuplicatedCode
    def enrich(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = self._tags_spec_to_list(tags_spec)
        tags = None if tags is None else set(self._tags_spec_to_list(tags_spec))
        dq_columns = self._columns(tags)

        schema = self._schema
        user_columns, system_columns = td_frame._split_columns(schema.names())

        for column in dq_columns:
            if column not in self._cols:
                raise DataQualityProcessingError(
                    ErrorCode.DQPE10,
                    column,
                )
        columns = list(user_columns) + sorted(dq_columns) + list(system_columns)

        enrich_lf = self._tf._lf.select(columns)

        enrich_tf = td_frame.TableFrame.__build__(
            df=enrich_lf,
            mode="tab",
            idx=self._tf._idx,
            properties=self._tf._properties,
        )

        return DataQualityEngine._new(
            tf=enrich_tf,
            dq_i=self,
            schema=None,
            cols={},
            tags=None,
        )

    # noinspection DuplicatedCode
    def summary(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = self._tags_spec_to_list(tags_spec)
        tags = None if tags is None else set(self._tags_spec_to_list(tags_spec))
        columns = self._columns(tags)

        dq_columns = []
        for column in columns:
            if column not in self._cols:
                raise DataQualityProcessingError(
                    ErrorCode.DQPE10,
                    column,
                )
            dq_columns.append(column)

        # noinspection PyProtectedMember
        lf = self._tf._lf.select(dq_columns)
        summary_data = []

        for column in sorted(dq_columns):
            column_info = self._cols[column]
            classifier_family = column_info.family
            summary_row = None

            match classifier_family:
                case ClassifierFamily.BOOLEAN:
                    false_count = (
                        lf.select((pl.col(column) == ByteSentinel.FALSE).sum())
                        .collect()
                        .item()
                    )
                    true_count = (
                        lf.select((pl.col(column) == ByteSentinel.TRUE).sum())
                        .collect()
                        .item()
                    )
                    nan_count = (
                        lf.select((pl.col(column) == ByteSentinel.NAN).sum())
                        .collect()
                        .item()
                    )
                    null_count = (
                        lf.select((pl.col(column) == ByteSentinel.NULL).sum())
                        .collect()
                        .item()
                    )
                    summary_row = {
                        DQColumns.DQ_VERSION.value: DQ_VERSION,
                        DQColumns.DQ_CLASSIFIER.value: column,
                        DQColumns.DQ_TYPE.value: ClassifierFamily.BOOLEAN.value,
                        DQColumns.DQ_BINS.value: None,
                        DQColumns.DQ_FALSE.value: false_count,
                        DQColumns.DQ_TRUE.value: true_count,
                        DQColumns.DQ_UNDERFLOW.value: None,
                        DQColumns.DQ_OVERFLOW.value: None,
                        DQColumns.DQ_NAN.value: nan_count,
                        DQColumns.DQ_NONE.value: null_count,
                    }
                    for i in range(MAX_BINS + 1):
                        summary_row[DQColumns.column(i)] = None
                case ClassifierFamily.CATEGORICAL:
                    categorizer = cast(Categorizer, column_info.classifier)
                    underflow_count = (
                        lf.select((pl.col(column) == ByteSentinel.UNDERFLOW).sum())
                        .collect()
                        .item()
                    )
                    overflow_count = (
                        lf.select((pl.col(column) == ByteSentinel.OVERFLOW).sum())
                        .collect()
                        .item()
                    )
                    nan_count = (
                        lf.select((pl.col(column) == ByteSentinel.NAN).sum())
                        .collect()
                        .item()
                    )
                    null_count = (
                        lf.select((pl.col(column) == ByteSentinel.NULL).sum())
                        .collect()
                        .item()
                    )
                    summary_row = {
                        DQColumns.DQ_VERSION.value: DQ_VERSION,
                        DQColumns.DQ_CLASSIFIER.value: column,
                        DQColumns.DQ_TYPE.value: ClassifierFamily.CATEGORICAL.value,
                        DQColumns.DQ_BINS.value: categorizer.scale.bins,
                        DQColumns.DQ_FALSE.value: None,
                        DQColumns.DQ_TRUE.value: None,
                        DQColumns.DQ_UNDERFLOW.value: underflow_count,
                        DQColumns.DQ_OVERFLOW.value: overflow_count,
                        DQColumns.DQ_NAN.value: nan_count,
                        DQColumns.DQ_NONE.value: null_count,
                    }
                    if categorizer.scale.min_in_first_bin:
                        summary_row[DQColumns.column(0)] = None
                    else:
                        count = lf.select((pl.col(column) == 0).sum()).collect().item()
                        summary_row[DQColumns.column(0)] = count
                    for i in range(1, categorizer.scale.bins + 1):
                        count = lf.select((pl.col(column) == i).sum()).collect().item()
                        summary_row[DQColumns.column(i)] = count
                    for i in range(categorizer.scale.bins + 1, MAX_BINS + 1):
                        summary_row[DQColumns.column(i)] = None
                case _:
                    raise ValueError(
                        f"Unsupported classifier family: {classifier_family}"
                    )

            summary_row[DQColumns.DQ_PARAMETERS.value] = None
            summary_data.append(summary_row)

        # Parquet format doesn't natively support 128-bit integer types.
        # We stick to 64-bit integers for now.
        schema = {
            DQColumns.DQ_VERSION.value: pl.UInt8,
            DQColumns.DQ_CLASSIFIER.value: pl.String,
            DQColumns.DQ_TYPE.value: pl.String,
            DQColumns.DQ_BINS.value: pl.UInt8,
            DQColumns.DQ_FALSE.value: pl.UInt64,
            DQColumns.DQ_TRUE.value: pl.UInt64,
            DQColumns.DQ_UNDERFLOW.value: pl.UInt64,
            DQColumns.DQ_OVERFLOW.value: pl.UInt64,
            DQColumns.DQ_NAN.value: pl.UInt64,
            DQColumns.DQ_NONE.value: pl.UInt64,
        }
        for i in range(MAX_BINS + 1):
            schema[DQColumns.column(i)] = pl.UInt64
        schema[DQColumns.DQ_PARAMETERS.value] = pl.String

        summary_lf = pl.DataFrame(summary_data, schema=schema).lazy()

        summary_tf = td_frame.TableFrame.__build__(
            df=summary_lf,
            mode="raw",
            idx=0,
            properties=TableFramePropertiesBuilder.empty(),
        )

        return DataQualityEngine._new(
            tf=summary_tf,
            dq_i=self,
            schema=None,
            cols={},
            tags=None,
        )

    # ToDo: Work in progress...
    # noinspection DuplicatedCode
    def filter(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = self._tags_spec_to_list(tags_spec)
        tags = None if tags is None else set(self._tags_spec_to_list(tags_spec))
        columns = self._columns(tags)

        dq_columns = list(columns)

        filter_lf = self._tf.select(dq_columns)

        return DataQualityEngine._new(
            tf=filter_lf,
            dq_i=self,
            schema=None,
            cols={},
            tags=None,
        )

    # ToDo: Work in progress...
    # noinspection DuplicatedCode
    def select(
        self,
        tags_spec: TagsSpec,
    ) -> Self:
        tags = self._tags_spec_to_list(tags_spec)
        tags = None if tags is None else set(self._tags_spec_to_list(tags_spec))
        columns = self._columns(tags)

        dq_columns = list(columns)

        select_lf = self._tf.select(dq_columns)

        return DataQualityEngine._new(
            tf=select_lf,
            dq_i=self,
            schema=None,
            cols={},
            tags=None,
        )

    # ToDo: Pending for future releases...
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

    # ToDo: Pending for future releases...
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
    IsPositive: ClassifierMetadata(
        column_suffix="_is_positive",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_positive),
    ),
    IsPositiveOrZero: ClassifierMetadata(
        column_suffix="_is_positive_or_zero",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_positive_or_zero),
    ),
    IsNegative: ClassifierMetadata(
        column_suffix="_is_negative",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_negative),
    ),
    IsNegativeOrZero: ClassifierMetadata(
        column_suffix="_is_negative_or_zero",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_negative_or_zero),
    ),
    IsBetween: ClassifierMetadata(
        column_suffix="_is_between",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_between),
    ),
    IsNotBetween: ClassifierMetadata(
        column_suffix="_is_not_between",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_between),
    ),
    IsIn: ClassifierMetadata(
        column_suffix="_is_in",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_in),
    ),
    IsNotIn: ClassifierMetadata(
        column_suffix="_is_not_in",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.is_not_in),
    ),
    Matches: ClassifierMetadata(
        column_suffix="_matches",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.matches),
    ),
    DoesNotMatch: ClassifierMetadata(
        column_suffix="_does_not_match",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.does_not_match),
    ),
    HasLength: ClassifierMetadata(
        column_suffix="_has_length",
        classifier_family=ClassifierFamily.BOOLEAN,
        polars_provider=PolarsProvider(PolarsProvider.has_length),
    ),
    Categorizer: ClassifierMetadata(
        column_suffix="_category",
        classifier_family=ClassifierFamily.CATEGORICAL,
        polars_provider=PolarsProvider(PolarsProvider.categorizer),
    ),
}
