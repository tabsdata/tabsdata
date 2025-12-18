#
# Copyright 2025 Tabs Data Inc.
#

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable

import polars as pl

import tabsdata as td
from tabsdata.dataquality import (
    DataQuality,
    IsNan,
    IsNotNan,
    IsNotNull,
    IsNull,
    Summary,
)
from tabsdata.dataquality._classifier import Classifier
from tabsdata.tableframe._actions.dataquality.engine import DataQualityEngine
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

NaN = "NaN"

Q_ZERO = 128
Q_POSITIVE = 64
Q_NEGATIVE = 32
Q_NULL = 16
Q_NaN = 8


@dataclass(frozen=True)
class DataShape:
    zero: int
    positive: int
    negative: int
    null: int
    nan: int
    converter: Callable[[Any], Any]

    @staticmethod
    def to_boolean(value):
        if value is None or value == NaN:
            return None
        return bool(value)

    @staticmethod
    def to_date(value):
        if value is None or value == NaN:
            return None
        return date(1914, 5, 19) + timedelta(days=int(value))

    @staticmethod
    def to_datetime(value):
        if value is None or value == NaN:
            return None
        return datetime(1914, 5, 19, 3, 14, 15) + timedelta(seconds=int(value))

    @staticmethod
    def to_decimal(value):
        if value is None or value == NaN:
            return None
        return Decimal(str(value))

    @staticmethod
    def to_duration(value):
        if value is None or value == NaN:
            return None
        return timedelta(seconds=int(value))

    @staticmethod
    def to_float(value):
        if value is None:
            return None
        elif value == NaN:
            return float(NaN)
        else:
            return float(value)

    @staticmethod
    def to_integer(value):
        if value is None or value == NaN:
            return None
        return int(value)

    @staticmethod
    def to_null(_):
        return None

    @staticmethod
    def to_string(value):
        if value is None or value == NaN:
            return None
        return str(value)

    @staticmethod
    def to_time(value):
        if value is None or value == NaN:
            return None
        return (datetime(1914, 5, 19, 3, 14, 15) + timedelta(seconds=int(value))).time()

    @staticmethod
    def to_unsigned_integer(value):
        if value is None or value == NaN:
            return None
        return abs(int(value))

    @staticmethod
    def to_utf(value):
        if value is None or value == NaN:
            return None
        return str(value)


@dataclass(frozen=True)
class ClassifierConfiguration:
    classifiers: list[Classifier]
    method: Callable[..., DataQualityEngine]
    checker: Callable[[pl.DataFrame, dict], None]
    actions: Callable[[str], DataQuality]
    dtypes: list[type]


def _is_null_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_null" in df.columns
    assert "null" in tags
    assert "value_is_null" in tags["null"]

    null_count = df.filter(pl.col("value").is_null()).height
    dq_true_count = df.filter(pl.col("value_is_null") == True).height
    assert (
        null_count == dq_true_count
    ), f"value: null_count={null_count}, dq_true_count={dq_true_count}"


def _is_not_null_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_not_null" in df.columns
    assert "not_null" in tags
    assert "value_is_not_null" in tags["not_null"]

    not_null_count = df.filter(pl.col("value").is_not_null()).height
    dq_true_count = df.filter(pl.col("value_is_not_null") == True).height
    assert (
        not_null_count == dq_true_count
    ), f"value: not_null_count={not_null_count}, dq_true_count={dq_true_count}"


def _is_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns

    if "nan" in tags:
        assert "value_is_nan" in df.columns
        assert "value_is_nan" in tags["nan"]
        nan_or_null_count = df.filter(
            pl.col("value").is_nan() | pl.col("value").is_null()
        ).height
        dq_true_count = df.filter(pl.col("value_is_nan") == True).height
        assert nan_or_null_count == dq_true_count, (
            f"value (nan): nan_or_null_count={nan_or_null_count},"
            f" dq_true_count={dq_true_count}"
        )

    if "nan_ignore_nulls" in tags:
        assert "value_is_nan_ignore_nulls" in df.columns
        assert "value_is_nan_ignore_nulls" in tags["nan_ignore_nulls"]
        nan_count = df.filter(
            pl.col("value").is_nan() & pl.col("value").is_not_null()
        ).height
        dq_true_count = df.filter(pl.col("value_is_nan_ignore_nulls") == True).height
        assert nan_count == dq_true_count, (
            f"value (nan_ignore_nulls): nan_count={nan_count},"
            f" dq_true_count={dq_true_count}"
        )


def _is_not_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns

    if "not_nan" in tags:
        assert "value_is_not_nan" in df.columns
        assert "value_is_not_nan" in tags["not_nan"]
        not_nan_not_null_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").is_not_null()
        ).height
        dq_true_count = df.filter(pl.col("value_is_not_nan") == True).height
        assert not_nan_not_null_count == dq_true_count, (
            f"value (not_nan): not_nan_not_null_count={not_nan_not_null_count},"
            f" dq_true_count={dq_true_count}"
        )

    if "not_nan_ignore_nulls" in tags:
        assert "value_is_not_nan_ignore_nulls" in df.columns
        assert "value_is_not_nan_ignore_nulls" in tags["not_nan_ignore_nulls"]
        not_nan_or_null_count = df.filter(
            pl.col("value").is_not_nan() | pl.col("value").is_null()
        ).height
        dq_true_count = df.filter(
            pl.col("value_is_not_nan_ignore_nulls") == True
        ).height
        assert not_nan_or_null_count == dq_true_count, (
            "value (not_nan_ignore_nulls):"
            f" not_nan_or_null_count={not_nan_or_null_count},"
            f" dq_true_count={dq_true_count}"
        )


def _is_null_action(table: str = "table00") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNull(
            column_names="value",
            tags="null",
        ),
        operators=Summary(),
    )


def _is_not_null_action(table: str = "table00") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNotNull(
            column_names="value",
            tags="not_null",
        ),
        operators=Summary(),
    )


def _is_nan_action(table: str = "table00") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNan(
                column_names=("value", "value_is_nan"),
                tags="nan",
                ignore_nulls=False,
            ),
            IsNan(
                column_names=("value", "value_is_nan_ignore_nulls"),
                tags="nan_ignore_nulls",
                ignore_nulls=True,
            ),
        ],
        operators=Summary(),
    )


def _is_not_nan_action(table: str = "table00") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNotNan(
                column_names=("value", "value_is_not_nan"),
                tags="not_nan",
                ignore_nulls=False,
            ),
            IsNotNan(
                column_names=("value", "value_is_not_nan_ignore_nulls"),
                tags="not_nan_ignore_nulls",
                ignore_nulls=True,
            ),
        ],
        operators=Summary(),
    )


DATASET_ROWS: dict[type, DataShape] = {
    td.Boolean: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        0,
        Q_NULL,
        0,
        DataShape.to_boolean,
    ),
    td.Date: DataShape(
        0,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_date,
    ),
    td.Datetime: DataShape(
        0,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_datetime,
    ),
    td.Decimal: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_decimal,
    ),
    td.Duration: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_duration,
    ),
    td.Float32: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        Q_NaN,
        DataShape.to_float,
    ),
    td.Float64: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        Q_NaN,
        DataShape.to_float,
    ),
    td.Int8: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_integer,
    ),
    td.Int16: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_integer,
    ),
    td.Int32: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_integer,
    ),
    td.Int64: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_integer,
    ),
    td.Int128: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_integer,
    ),
    td.Null: DataShape(
        0,
        0,
        0,
        Q_NULL,
        0,
        DataShape.to_null,
    ),
    td.String: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_string,
    ),
    td.Time: DataShape(
        0,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_time,
    ),
    td.UInt8: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        0,
        Q_NULL,
        0,
        DataShape.to_unsigned_integer,
    ),
    td.UInt16: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        0,
        Q_NULL,
        0,
        DataShape.to_unsigned_integer,
    ),
    td.UInt32: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        0,
        Q_NULL,
        0,
        DataShape.to_unsigned_integer,
    ),
    td.UInt64: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        0,
        Q_NULL,
        0,
        DataShape.to_unsigned_integer,
    ),
    td.Utf8: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        0,
        DataShape.to_utf,
    ),
}


NULL_CAPABLE_DTYPES = list(DATASET_ROWS.keys())

NAN_CAPABLE_DTYPES = [td.Float32, td.Float64]

CLASSIFIERS: dict[type[Classifier], ClassifierConfiguration] = {
    IsNull: ClassifierConfiguration(
        classifiers=[
            IsNull(
                column_names="value",
                tags="null",
            )
        ],
        method=DataQualityEngine.is_null,
        checker=_is_null_checker,
        actions=_is_null_action,
        dtypes=NULL_CAPABLE_DTYPES,
    ),
    IsNotNull: ClassifierConfiguration(
        classifiers=[
            IsNotNull(
                column_names="value",
                tags="not_null",
            )
        ],
        method=DataQualityEngine.is_not_null,
        checker=_is_not_null_checker,
        actions=_is_not_null_action,
        dtypes=NULL_CAPABLE_DTYPES,
    ),
    IsNan: ClassifierConfiguration(
        classifiers=[
            IsNan(
                column_names=("value", "value_is_nan"),
                tags="nan",
                ignore_nulls=False,
            ),
            IsNan(
                column_names=("value", "value_is_nan_ignore_nulls"),
                tags="nan_ignore_nulls",
                ignore_nulls=True,
            ),
        ],
        method=DataQualityEngine.is_nan,
        checker=_is_nan_checker,
        actions=_is_nan_action,
        dtypes=NAN_CAPABLE_DTYPES,
    ),
    IsNotNan: ClassifierConfiguration(
        classifiers=[
            IsNotNan(
                column_names=("value", "value_is_not_nan"),
                tags="not_nan",
                ignore_nulls=False,
            ),
            IsNotNan(
                column_names=("value", "value_is_not_nan_ignore_nulls"),
                tags="not_nan_ignore_nulls",
                ignore_nulls=True,
            ),
        ],
        method=DataQualityEngine.is_not_nan,
        checker=_is_not_nan_checker,
        actions=_is_not_nan_action,
        dtypes=NAN_CAPABLE_DTYPES,
    ),
}


def dataset(dtype) -> td.TableFrame:
    if dtype not in DATASET_ROWS:
        raise ValueError(f"Unsupported dtype: {dtype}")

    quantities = DATASET_ROWS[dtype]
    converter = quantities.converter

    data_values = (
        [converter(0)] * quantities.zero
        + [converter(1000)] * quantities.positive
        + [converter(-1000)] * quantities.negative
        + [converter(None)] * quantities.null
        + [converter(NaN)] * quantities.nan
    )

    total_rows = len(data_values)
    data = {
        "id": list(range(total_rows)),
        "value": data_values,
    }

    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    return tf
