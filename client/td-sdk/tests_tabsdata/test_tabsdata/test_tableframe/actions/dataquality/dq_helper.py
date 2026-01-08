#
# Copyright 2025 Tabs Data Inc.
#

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Literal

import polars as pl

import tabsdata as td
from tabsdata.dataquality import (
    DataQuality,
    DoesNotMatch,
    HasLength,
    IsBetween,
    IsIn,
    IsNan,
    IsNegative,
    IsNotBetween,
    IsNotIn,
    IsNotNan,
    IsNotNull,
    IsNull,
    IsPositive,
    IsPositiveOrZero,
    IsZero,
    Matches,
    Summary,
)
from tabsdata.dataquality._classifier import (
    Classifier,
    IsNegativeOrZero,
    IsNotNullNorNan,
    IsNotZero,
    IsNullOrNan,
)
from tabsdata.dataquality._typing import RangeClosedOnSpec
from tabsdata.tableframe._actions.dataquality.engine import (
    CLOSED_MAPPING,
    ByteSentinel,
    DataQualityEngine,
)
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
    name: str | None = None


def _is_null_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_null" in df.columns
    assert "null" in tags
    assert "value_is_null" in tags["null"]
    assert (
        df.schema["value_is_null"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_null']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    true_count = df.filter(pl.col("value").is_null()).height
    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_null() & pl.col("value").is_not_nan()
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").is_not_null()).height
        nan_count = 0
    null_count = 0

    dq_false_count = df.filter(pl.col("value_is_null") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_null") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_null") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_null") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_null FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_null TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_null NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_null NULL: dq={dq_null_count}, expected={null_count}"


def _is_not_null_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_not_null" in df.columns
    assert "not_null" in tags
    assert "value_is_not_null" in tags["not_null"]
    assert df.schema["value_is_not_null"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_null']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    false_count = df.filter(pl.col("value").is_null()).height
    if is_float:
        true_count = df.filter(
            pl.col("value").is_not_null() & pl.col("value").is_not_nan()
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        true_count = df.filter(pl.col("value").is_not_null()).height
        nan_count = 0
    null_count = 0

    dq_false_count = df.filter(pl.col("value_is_not_null") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_not_null") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_not_null") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_not_null") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_not_null FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_not_null TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_not_null NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_not_null NULL: dq={dq_null_count}, expected={null_count}"


def _is_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_nan" in df.columns
    assert "nan" in tags
    assert "value_is_nan" in tags["nan"]
    assert (
        df.schema["value_is_nan"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_nan']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]
    assert (
        is_float
    ), f"is_nan checker should only be called for Float types; got {dtype}"

    true_count = df.filter(pl.col("value").is_nan()).height
    false_count = df.filter(
        pl.col("value").is_not_null() & pl.col("value").is_not_nan()
    ).height
    nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_nan") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_nan") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_nan") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_nan") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_nan FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_nan TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_nan NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_nan NULL: dq={dq_null_count}, expected={null_count}"


def _is_not_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_not_nan" in df.columns
    assert "not_nan" in tags
    assert "value_is_not_nan" in tags["not_nan"]
    assert (
        df.schema["value_is_not_nan"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_not_nan']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]
    assert (
        is_float
    ), f"is_not_nan checker should only be called for Float types; got {dtype}"

    false_count = df.filter(pl.col("value").is_nan()).height
    true_count = df.filter(
        pl.col("value").is_not_null() & pl.col("value").is_not_nan()
    ).height
    null_count = df.filter(pl.col("value").is_null()).height
    nan_count = 0

    dq_false_count = df.filter(pl.col("value_is_not_nan") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_not_nan") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_not_nan") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_not_nan") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_not_nan FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_not_nan TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_not_nan NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_not_nan NULL: dq={dq_null_count}, expected={null_count}"


def _is_null_or_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_null_or_nan" in df.columns
    assert "null_or_nan" in tags
    assert "value_is_null_or_nan" in tags["null_or_nan"]
    assert df.schema["value_is_null_or_nan"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_null_or_nan']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]
    assert (
        is_float
    ), f"is_null_or_nan checker should only be called for Float types; got {dtype}"

    true_count = df.filter(pl.col("value").is_null() | pl.col("value").is_nan()).height
    false_count = df.filter(
        pl.col("value").is_not_null() & pl.col("value").is_not_nan()
    ).height
    nan_count = 0
    null_count = 0

    dq_false_count = df.filter(
        pl.col("value_is_null_or_nan") == ByteSentinel.FALSE
    ).height
    dq_true_count = df.filter(
        pl.col("value_is_null_or_nan") == ByteSentinel.TRUE
    ).height
    dq_nan_count = df.filter(pl.col("value_is_null_or_nan") == ByteSentinel.NAN).height
    dq_null_count = df.filter(
        pl.col("value_is_null_or_nan") == ByteSentinel.NULL
    ).height

    assert (
        dq_false_count == false_count
    ), f"is_null_or_nan FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_null_or_nan TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_null_or_nan NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_null_or_nan NULL: dq={dq_null_count}, expected={null_count}"


def _is_not_null_nor_nan_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_not_null_nor_nan" in df.columns
    assert "not_null_nor_nan" in tags
    assert "value_is_not_null_nor_nan" in tags["not_null_nor_nan"]
    assert df.schema["value_is_not_null_nor_nan"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_null_nor_nan']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]
    assert (
        is_float
    ), f"is_not_null_nor_nan checker should only be called for Float types; got {dtype}"

    true_count = df.filter(
        pl.col("value").is_not_null() & pl.col("value").is_not_nan()
    ).height
    false_count = df.filter(pl.col("value").is_null() | pl.col("value").is_nan()).height
    nan_count = 0
    null_count = 0

    dq_false_count = df.filter(
        pl.col("value_is_not_null_nor_nan") == ByteSentinel.FALSE
    ).height
    dq_true_count = df.filter(
        pl.col("value_is_not_null_nor_nan") == ByteSentinel.TRUE
    ).height
    dq_nan_count = df.filter(
        pl.col("value_is_not_null_nor_nan") == ByteSentinel.NAN
    ).height
    dq_null_count = df.filter(
        pl.col("value_is_not_null_nor_nan") == ByteSentinel.NULL
    ).height

    assert (
        dq_false_count == false_count
    ), f"is_not_null_nor_nan FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_not_null_nor_nan TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_not_null_nor_nan NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_not_null_nor_nan NULL: dq={dq_null_count}, expected={null_count}"


def _is_zero_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_zero" in df.columns
    assert "zero" in tags
    assert "value_is_zero" in tags["zero"]
    assert (
        df.schema["value_is_zero"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_zero']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").ne(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").eq(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").ne(0)).height
        true_count = df.filter(pl.col("value").eq(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_zero") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_zero") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_zero") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_zero") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_zero FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_zero TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_zero NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_zero NULL: dq={dq_null_count}, expected={null_count}"


def _is_not_zero_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_not_zero" in df.columns
    assert "not_zero" in tags
    assert "value_is_not_zero" in tags["not_zero"]
    assert df.schema["value_is_not_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").eq(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").ne(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").eq(0)).height
        true_count = df.filter(pl.col("value").ne(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_not_zero") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_not_zero") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_not_zero") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_not_zero") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_not_zero FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_not_zero TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_not_zero NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_not_zero NULL: dq={dq_null_count}, expected={null_count}"


def _is_positive_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_positive" in df.columns
    assert "positive" in tags
    assert "value_is_positive" in tags["positive"]
    assert df.schema["value_is_positive"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_positive']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").le(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").gt(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").le(0)).height
        true_count = df.filter(pl.col("value").gt(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_positive") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_positive") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_positive") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_positive") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_positive FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_positive TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_positive NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_positive NULL: dq={dq_null_count}, expected={null_count}"


def _is_positive_or_zero_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_positive_or_zero" in df.columns
    assert "positive_or_zero" in tags
    assert "value_is_positive_or_zero" in tags["positive_or_zero"]
    assert df.schema["value_is_positive_or_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_positive_or_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").lt(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").ge(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").lt(0)).height
        true_count = df.filter(pl.col("value").ge(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(
        pl.col("value_is_positive_or_zero") == ByteSentinel.FALSE
    ).height
    dq_true_count = df.filter(
        pl.col("value_is_positive_or_zero") == ByteSentinel.TRUE
    ).height
    dq_nan_count = df.filter(
        pl.col("value_is_positive_or_zero") == ByteSentinel.NAN
    ).height
    dq_null_count = df.filter(
        pl.col("value_is_positive_or_zero") == ByteSentinel.NULL
    ).height

    assert (
        dq_false_count == false_count
    ), f"is_positive_or_zero FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_positive_or_zero TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_positive_or_zero NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_positive_or_zero NULL: dq={dq_null_count}, expected={null_count}"


def _is_negative_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_negative" in df.columns
    assert "negative" in tags
    assert "value_is_negative" in tags["negative"]
    assert df.schema["value_is_negative"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_negative']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").ge(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").lt(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").ge(0)).height
        true_count = df.filter(pl.col("value").lt(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_negative") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_negative") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_negative") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_negative") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_negative FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_negative TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_negative NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_negative NULL: dq={dq_null_count}, expected={null_count}"


def _is_negative_or_zero_checker(df: pl.DataFrame, tags: dict):
    assert "value" in df.columns
    assert "value_is_negative_or_zero" in df.columns
    assert "negative_or_zero" in tags
    assert "value_is_negative_or_zero" in tags["negative_or_zero"]
    assert df.schema["value_is_negative_or_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_negative_or_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float32, pl.Float64]

    if is_float:
        false_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").gt(0)
        ).height
        true_count = df.filter(
            pl.col("value").is_not_nan() & pl.col("value").le(0)
        ).height
        nan_count = df.filter(pl.col("value").is_nan()).height
    else:
        false_count = df.filter(pl.col("value").gt(0)).height
        true_count = df.filter(pl.col("value").le(0)).height
        nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(
        pl.col("value_is_negative_or_zero") == ByteSentinel.FALSE
    ).height
    dq_true_count = df.filter(
        pl.col("value_is_negative_or_zero") == ByteSentinel.TRUE
    ).height
    dq_nan_count = df.filter(
        pl.col("value_is_negative_or_zero") == ByteSentinel.NAN
    ).height
    dq_null_count = df.filter(
        pl.col("value_is_negative_or_zero") == ByteSentinel.NULL
    ).height

    assert (
        dq_false_count == false_count
    ), f"is_negative_or_zero FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_negative_or_zero TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_negative_or_zero NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_negative_or_zero NULL: dq={dq_null_count}, expected={null_count}"


def _make_is_between_checker(
    min_val: Any, max_val: Any, closed: RangeClosedOnSpec = "both"
):
    def _is_between_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_is_between" in df.columns
        assert "is_between" in tags
        assert "value_is_between" in tags["is_between"]
        assert df.schema["value_is_between"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_between']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        converted_min = converter(min_val) if min_val is not None else None
        converted_max = converter(max_val) if max_val is not None else None

        polars_closed = CLOSED_MAPPING[closed]

        if min_val is None and max_val is None:
            if is_float:
                false_count = 0
                true_count = df.filter(
                    pl.col("value").is_not_nan() & pl.col("value").is_not_null()
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = 0
                true_count = df.filter(pl.col("value").is_not_null()).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        elif min_val is None:
            if closed in ("both", "upper"):
                condition = pl.col("value").le(converted_max)
            else:
                condition = pl.col("value").lt(converted_max)
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                true_count = df.filter(pl.col("value").is_not_null() & condition).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        elif max_val is None:
            if closed in ("both", "lower"):
                condition = pl.col("value").ge(converted_min)
            else:
                condition = pl.col("value").gt(converted_min)
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                true_count = df.filter(pl.col("value").is_not_null() & condition).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        else:
            condition = pl.col("value").is_between(
                converted_min, converted_max, closed=polars_closed
            )
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                true_count = df.filter(pl.col("value").is_not_null() & condition).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(
            pl.col("value_is_between") == ByteSentinel.FALSE
        ).height
        dq_true_count = df.filter(
            pl.col("value_is_between") == ByteSentinel.TRUE
        ).height
        dq_nan_count = df.filter(pl.col("value_is_between") == ByteSentinel.NAN).height
        dq_null_count = df.filter(
            pl.col("value_is_between") == ByteSentinel.NULL
        ).height

        assert (
            dq_false_count == false_count
        ), f"is_between FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"is_between TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"is_between NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"is_between NULL: dq={dq_null_count}, expected={null_count}"

    return _is_between_checker


def _make_is_not_between_checker(
    min_val: Any, max_val: Any, closed: RangeClosedOnSpec = "both"
):
    def _is_not_between_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_is_not_between" in df.columns
        assert "is_not_between" in tags
        assert "value_is_not_between" in tags["is_not_between"]
        assert df.schema["value_is_not_between"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_not_between']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        converted_min = converter(min_val) if min_val is not None else None
        converted_max = converter(max_val) if max_val is not None else None

        polars_closed = CLOSED_MAPPING[closed]

        if min_val is None and max_val is None:
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan() & pl.col("value").is_not_null()
                ).height
                true_count = 0
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(pl.col("value").is_not_null()).height
                true_count = 0
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        elif min_val is None:
            if closed in ("both", "upper"):
                condition = pl.col("value").le(converted_max)
            else:
                condition = pl.col("value").lt(converted_max)
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        elif max_val is None:
            if closed in ("both", "lower"):
                condition = pl.col("value").ge(converted_min)
            else:
                condition = pl.col("value").gt(converted_min)
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
        else:
            condition = pl.col("value").is_between(
                converted_min, converted_max, closed=polars_closed
            )
            if is_float:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~condition
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & condition
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & ~condition
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(
            pl.col("value_is_not_between") == ByteSentinel.FALSE
        ).height
        dq_true_count = df.filter(
            pl.col("value_is_not_between") == ByteSentinel.TRUE
        ).height
        dq_nan_count = df.filter(
            pl.col("value_is_not_between") == ByteSentinel.NAN
        ).height
        dq_null_count = df.filter(
            pl.col("value_is_not_between") == ByteSentinel.NULL
        ).height

        assert (
            dq_false_count == false_count
        ), f"is_not_between FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"is_not_between TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"is_not_between NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"is_not_between NULL: dq={dq_null_count}, expected={null_count}"

    return _is_not_between_checker


def _make_is_in_checker(values_set: set):
    def _is_in_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_is_in" in df.columns
        assert "is_in" in tags
        assert "value_is_in" in tags["is_in"]
        assert (
            df.schema["value_is_in"] == pl.UInt8
        ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_in']}"

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        values = {converter(value) for value in values_set}

        has_none = None in values
        has_nan = any(
            isinstance(value, float) and math.isnan(value) for value in values
        )

        if is_float:
            if has_none and has_nan:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_null()
                    | pl.col("value").is_nan()
                    | pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = 0
            elif has_none:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_null()
                    | (
                        pl.col("value").is_not_nan()
                        & pl.col("value").is_in(list(values))
                    )
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = 0
            elif has_nan:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_nan()
                    | (
                        pl.col("value").is_not_nan()
                        & pl.col("value").is_not_null()
                        & pl.col("value").is_in(list(values))
                    )
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & pl.col("value").is_in(list(values))
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
        else:
            if has_none:
                false_count = df.filter(
                    pl.col("value").is_not_null() & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_null() | pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = 0
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & ~pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(pl.col("value_is_in") == ByteSentinel.FALSE).height
        dq_true_count = df.filter(pl.col("value_is_in") == ByteSentinel.TRUE).height
        dq_nan_count = df.filter(pl.col("value_is_in") == ByteSentinel.NAN).height
        dq_null_count = df.filter(pl.col("value_is_in") == ByteSentinel.NULL).height

        assert (
            dq_false_count == false_count
        ), f"is_in FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"is_in TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"is_in NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"is_in NULL: dq={dq_null_count}, expected={null_count}"

    return _is_in_checker


def _make_is_not_in_checker(values_set: set):
    def _is_not_in_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_is_not_in" in df.columns
        assert "is_not_in" in tags
        assert "value_is_not_in" in tags["is_not_in"]
        assert df.schema["value_is_not_in"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_not_in']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        values = {converter(value) for value in values_set}

        has_none = None in values
        has_nan = any(
            isinstance(value, float) and math.isnan(value) for value in values
        )

        if is_float:
            if has_none and has_nan:
                false_count = df.filter(
                    pl.col("value").is_null()
                    | pl.col("value").is_nan()
                    | pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = 0
            elif has_none:
                false_count = df.filter(
                    pl.col("value").is_null()
                    | (
                        pl.col("value").is_not_nan()
                        & pl.col("value").is_in(list(values))
                    )
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = 0
            elif has_nan:
                false_count = df.filter(
                    pl.col("value").is_nan()
                    | (
                        pl.col("value").is_not_nan()
                        & pl.col("value").is_not_null()
                        & pl.col("value").is_in(list(values))
                    )
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height
            else:
                false_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_nan()
                    & pl.col("value").is_not_null()
                    & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = df.filter(pl.col("value").is_nan()).height
                null_count = df.filter(pl.col("value").is_null()).height
        else:
            if has_none:
                false_count = df.filter(
                    pl.col("value").is_null() | pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = 0
            else:
                false_count = df.filter(
                    pl.col("value").is_not_null() & pl.col("value").is_in(list(values))
                ).height
                true_count = df.filter(
                    pl.col("value").is_not_null() & ~pl.col("value").is_in(list(values))
                ).height
                nan_count = 0
                null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(
            pl.col("value_is_not_in") == ByteSentinel.FALSE
        ).height
        dq_true_count = df.filter(pl.col("value_is_not_in") == ByteSentinel.TRUE).height
        dq_nan_count = df.filter(pl.col("value_is_not_in") == ByteSentinel.NAN).height
        dq_null_count = df.filter(pl.col("value_is_not_in") == ByteSentinel.NULL).height

        assert (
            dq_false_count == false_count
        ), f"is_not_in FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"is_not_in TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"is_not_in NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"is_not_in NULL: dq={dq_null_count}, expected={null_count}"

    return _is_not_in_checker


def _make_matches_checker(pattern: str):
    def _matches_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_matches" in df.columns
        assert "matches" in tags
        assert "value_matches" in tags["matches"]
        assert df.schema["value_matches"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_matches']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float32, pl.Float64]

        if is_float:
            false_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & ~pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            nan_count = df.filter(pl.col("value").is_nan()).height
        else:
            false_count = df.filter(
                pl.col("value").is_not_null()
                & ~pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_null()
                & pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            nan_count = 0
        null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(pl.col("value_matches") == ByteSentinel.FALSE).height
        dq_true_count = df.filter(pl.col("value_matches") == ByteSentinel.TRUE).height
        dq_nan_count = df.filter(pl.col("value_matches") == ByteSentinel.NAN).height
        dq_null_count = df.filter(pl.col("value_matches") == ByteSentinel.NULL).height

        assert (
            dq_false_count == false_count
        ), f"matches FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"matches TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"matches NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"matches NULL: dq={dq_null_count}, expected={null_count}"

    return _matches_checker


def _make_does_not_match_checker(pattern: str):
    def _does_not_match_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_does_not_match" in df.columns
        assert "does_not_match" in tags
        assert "value_does_not_match" in tags["does_not_match"]
        assert df.schema["value_does_not_match"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_does_not_match']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float32, pl.Float64]

        if is_float:
            false_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & ~pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            nan_count = df.filter(pl.col("value").is_nan()).height
        else:
            false_count = df.filter(
                pl.col("value").is_not_null()
                & pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_null()
                & ~pl.col("value").cast(pl.Utf8).str.contains(pattern)
            ).height
            nan_count = 0
        null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(
            pl.col("value_does_not_match") == ByteSentinel.FALSE
        ).height
        dq_true_count = df.filter(
            pl.col("value_does_not_match") == ByteSentinel.TRUE
        ).height
        dq_nan_count = df.filter(
            pl.col("value_does_not_match") == ByteSentinel.NAN
        ).height
        dq_null_count = df.filter(
            pl.col("value_does_not_match") == ByteSentinel.NULL
        ).height

        assert (
            dq_false_count == false_count
        ), f"does_not_match FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"does_not_match TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"does_not_match NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"does_not_match NULL: dq={dq_null_count}, expected={null_count}"

    return _does_not_match_checker


def _make_has_length_checker(min_length: int, max_length: int):
    def _has_length_checker(df: pl.DataFrame, tags: dict):
        assert "value" in df.columns
        assert "value_has_length" in df.columns
        assert "has_length" in tags
        assert "value_has_length" in tags["has_length"]
        assert df.schema["value_has_length"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_has_length']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float32, pl.Float64]

        if is_float:
            false_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & ~pl.col("value")
                .cast(pl.Utf8)
                .str.len_chars()
                .is_between(min_length, max_length)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_nan()
                & pl.col("value").is_not_null()
                & pl.col("value")
                .cast(pl.Utf8)
                .str.len_chars()
                .is_between(min_length, max_length)
            ).height
            nan_count = df.filter(pl.col("value").is_nan()).height
        else:
            false_count = df.filter(
                pl.col("value").is_not_null()
                & ~pl.col("value")
                .cast(pl.Utf8)
                .str.len_chars()
                .is_between(min_length, max_length)
            ).height
            true_count = df.filter(
                pl.col("value").is_not_null()
                & pl.col("value")
                .cast(pl.Utf8)
                .str.len_chars()
                .is_between(min_length, max_length)
            ).height
            nan_count = 0
        null_count = df.filter(pl.col("value").is_null()).height

        dq_false_count = df.filter(
            pl.col("value_has_length") == ByteSentinel.FALSE
        ).height
        dq_true_count = df.filter(
            pl.col("value_has_length") == ByteSentinel.TRUE
        ).height
        dq_nan_count = df.filter(pl.col("value_has_length") == ByteSentinel.NAN).height
        dq_null_count = df.filter(
            pl.col("value_has_length") == ByteSentinel.NULL
        ).height

        assert (
            dq_false_count == false_count
        ), f"has_length FALSE: dq={dq_false_count}, expected={false_count}"
        assert (
            dq_true_count == true_count
        ), f"has_length TRUE: dq={dq_true_count}, expected={true_count}"
        assert (
            dq_nan_count == nan_count
        ), f"has_length NAN: dq={dq_nan_count}, expected={nan_count}"
        assert (
            dq_null_count == null_count
        ), f"has_length NULL: dq={dq_null_count}, expected={null_count}"

    return _has_length_checker


def _is_null_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNull(
            column_names=("value", "value_is_null"),
            tags="null",
        ),
        operators=Summary(),
    )


def _is_not_null_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNotNull(
            column_names=("value", "value_is_not_null"),
            tags="not_null",
        ),
        operators=Summary(),
    )


def _is_nan_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNan(
                column_names=("value", "value_is_nan"),
                tags="nan",
            ),
        ],
        operators=Summary(),
    )


def _is_not_nan_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNotNan(
                column_names=("value", "value_is_not_nan"),
                tags="not_nan",
            ),
        ],
        operators=Summary(),
    )


def _is_null_or_nan_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNullOrNan(
                column_names=("value", "value_is_null_or_nan"),
                tags="null_or_nan",
            ),
        ],
        operators=Summary(),
    )


def _is_not_null_nor_nan_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[
            IsNotNullNorNan(
                column_names=("value", "value_is_not_null_nor_nan"),
                tags="not_null_nor_nan",
            ),
        ],
        operators=Summary(),
    )


def _is_zero_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsZero(
            column_names=("value", "value_is_zero"),
            tags="zero",
        ),
        operators=Summary(),
    )


def _is_not_zero_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNotZero(
            column_names=("value", "value_is_not_zero"),
            tags="not_zero",
        ),
        operators=Summary(),
    )


def _is_positive_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsPositive(
            column_names=("value", "value_is_positive"),
            tags="positive",
        ),
        operators=Summary(),
    )


def _is_positive_or_zero_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsPositiveOrZero(
            column_names=("value", "value_is_positive_or_zero"),
            tags="positive_or_zero",
        ),
        operators=Summary(),
    )


def _is_negative_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNegative(
            column_names=("value", "value_is_negative"),
            tags="negative",
        ),
        operators=Summary(),
    )


def _is_negative_or_zero_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsNegativeOrZero(
            column_names=("value", "value_is_negative_or_zero"),
            tags="negative_or_zero",
        ),
        operators=Summary(),
    )


def _make_is_between_action(
    min_val: Any, max_val: Any, closed: RangeClosedOnSpec = "both"
):
    def _is_between_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:
            converter = DATASET_ROWS[dtype].converter
            converted_min = converter(min_val) if min_val is not None else None
            converted_max = converter(max_val) if max_val is not None else None
        else:
            converted_min = min_val
            converted_max = max_val

        return DataQuality(
            table=table,
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=converted_min,
                    max=converted_max,
                    closed=closed,
                ),
            ],
            operators=Summary(),
        )

    return _is_between_action


def _make_is_not_between_action(
    min_val: Any, max_val: Any, closed: RangeClosedOnSpec = "both"
):
    def _is_not_between_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:
            converter = DATASET_ROWS[dtype].converter
            converted_min = converter(min_val) if min_val is not None else None
            converted_max = converter(max_val) if max_val is not None else None
        else:
            converted_min = min_val
            converted_max = max_val

        return DataQuality(
            table=table,
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=converted_min,
                    max=converted_max,
                    closed=closed,
                ),
            ],
            operators=Summary(),
        )

    return _is_not_between_action


def _make_is_in_action(values_set: set):
    def _is_in_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:
            converter = DATASET_ROWS[dtype].converter
            converted_values = set()
            for value in values_set:
                if value is None:
                    converted_values.add(None)
                elif isinstance(value, float) and math.isnan(value):
                    converted_values.add(value)
                else:
                    converted_values.add(converter(value))
            converted_values = frozenset(converted_values)
        else:
            converted_values = frozenset(values_set)

        return DataQuality(
            table=table,
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=converted_values,
                ),
            ],
            operators=Summary(),
        )

    return _is_in_action


def _make_is_not_in_action(values_set: set):
    def _is_not_in_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:
            converter = DATASET_ROWS[dtype].converter
            converted_values = set()
            for value in values_set:
                if value is None:
                    converted_values.add(None)
                elif isinstance(value, float) and math.isnan(value):
                    converted_values.add(value)
                else:
                    converted_values.add(converter(value))
            converted_values = frozenset(converted_values)
        else:
            converted_values = frozenset(values_set)

        return DataQuality(
            table=table,
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=converted_values,
                ),
            ],
            operators=Summary(),
        )

    return _is_not_in_action


def _make_matches_action(pattern: str):
    def _matches_action(
        table: str = "table00",
        _dtype: type = None,
    ) -> DataQuality:
        return DataQuality(
            table=table,
            classifiers=[
                Matches(
                    column_names=("value", "value_matches"),
                    tags="matches",
                    pattern=pattern,
                ),
            ],
            operators=Summary(),
        )

    return _matches_action


def _make_does_not_match_action(pattern: str):
    def _does_not_match_action(
        table: str = "table00",
        _dtype: type = None,
    ) -> DataQuality:
        return DataQuality(
            table=table,
            classifiers=[
                DoesNotMatch(
                    column_names=("value", "value_does_not_match"),
                    tags="does_not_match",
                    pattern=pattern,
                ),
            ],
            operators=Summary(),
        )

    return _does_not_match_action


def _make_has_length_action(min_length: int, max_length: int):
    def _has_length_action(
        table: str = "table00",
        _dtype: type = None,
    ) -> DataQuality:
        return DataQuality(
            table=table,
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min=min_length,
                    max=max_length,
                ),
            ],
            operators=Summary(),
        )

    return _has_length_action


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

NAN_CAPABLE_DTYPES = [
    td.Float32,
    td.Float64,
]

NULL_AND_NAN_CAPABLE_DTYPES = list(set(NAN_CAPABLE_DTYPES) & set(NULL_CAPABLE_DTYPES))

NUMERIC_DTYPES = [
    td.Decimal,
    td.Int8,
    td.Int16,
    td.Int32,
    td.Int64,
    td.Int128,
    td.UInt8,
    td.UInt16,
    td.UInt32,
    td.UInt64,
    td.Float32,
    td.Float64,
]

SIGNED_NUMERIC_DTYPES = [
    td.Decimal,
    td.Int8,
    td.Int16,
    td.Int32,
    td.Int64,
    td.Int128,
    td.Float32,
    td.Float64,
]

STRING_DTYPES = [
    td.String,
    td.Utf8,
]

CLASSIFIERS: dict[type[Classifier], list[ClassifierConfiguration]] = {
    IsNull: [
        ClassifierConfiguration(
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
    ],
    IsNotNull: [
        ClassifierConfiguration(
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
    ],
    IsNan: [
        ClassifierConfiguration(
            classifiers=[
                IsNan(
                    column_names=("value", "value_is_nan"),
                    tags="nan",
                ),
            ],
            method=DataQualityEngine.is_nan,
            checker=_is_nan_checker,
            actions=_is_nan_action,
            dtypes=NAN_CAPABLE_DTYPES,
        ),
    ],
    IsNotNan: [
        ClassifierConfiguration(
            classifiers=[
                IsNotNan(
                    column_names=("value", "value_is_not_nan"),
                    tags="not_nan",
                ),
            ],
            method=DataQualityEngine.is_not_nan,
            checker=_is_not_nan_checker,
            actions=_is_not_nan_action,
            dtypes=NAN_CAPABLE_DTYPES,
        ),
    ],
    IsNullOrNan: [
        ClassifierConfiguration(
            classifiers=[
                IsNullOrNan(
                    column_names=("value", "value_is_null_or_nan"),
                    tags="null_or_nan",
                ),
            ],
            method=DataQualityEngine.is_null_or_nan,
            checker=_is_null_or_nan_checker,
            actions=_is_null_or_nan_action,
            dtypes=NULL_AND_NAN_CAPABLE_DTYPES,
        ),
    ],
    IsNotNullNorNan: [
        ClassifierConfiguration(
            classifiers=[
                IsNotNullNorNan(
                    column_names=("value", "value_is_not_null_nor_nan"),
                    tags="not_null_nor_nan",
                ),
            ],
            method=DataQualityEngine.is_not_null_nor_nan,
            checker=_is_not_null_nor_nan_checker,
            actions=_is_not_null_nor_nan_action,
            dtypes=NULL_AND_NAN_CAPABLE_DTYPES,
        ),
    ],
    IsZero: [
        ClassifierConfiguration(
            classifiers=[
                IsZero(
                    column_names=("value", "value_is_zero"),
                    tags="zero",
                ),
            ],
            method=DataQualityEngine.is_zero,
            checker=_is_zero_checker,
            actions=_is_zero_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsNotZero: [
        ClassifierConfiguration(
            classifiers=[
                IsNotZero(
                    column_names=("value", "value_is_not_zero"),
                    tags="not_zero",
                ),
            ],
            method=DataQualityEngine.is_not_zero,
            checker=_is_not_zero_checker,
            actions=_is_not_zero_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsPositive: [
        ClassifierConfiguration(
            classifiers=[
                IsPositive(
                    column_names=("value", "value_is_positive"),
                    tags="positive",
                ),
            ],
            method=DataQualityEngine.is_positive,
            checker=_is_positive_checker,
            actions=_is_positive_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsPositiveOrZero: [
        ClassifierConfiguration(
            classifiers=[
                IsPositiveOrZero(
                    column_names=("value", "value_is_positive_or_zero"),
                    tags="positive_or_zero",
                ),
            ],
            method=DataQualityEngine.is_positive_or_zero,
            checker=_is_positive_or_zero_checker,
            actions=_is_positive_or_zero_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsNegative: [
        ClassifierConfiguration(
            classifiers=[
                IsNegative(
                    column_names=("value", "value_is_negative"),
                    tags="negative",
                ),
            ],
            method=DataQualityEngine.is_negative,
            checker=_is_negative_checker,
            actions=_is_negative_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsNegativeOrZero: [
        ClassifierConfiguration(
            classifiers=[
                IsNegativeOrZero(
                    column_names=("value", "value_is_negative_or_zero"),
                    tags="negative_or_zero",
                ),
            ],
            method=DataQualityEngine.is_negative_or_zero,
            checker=_is_negative_or_zero_checker,
            actions=_is_negative_or_zero_action,
            dtypes=NUMERIC_DTYPES,
        ),
    ],
    IsBetween: [
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=-100,
                    max=100,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(-100, 100, "both"),
            actions=_make_is_between_action(-100, 100, "both"),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="range_-100_100_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=0,
                    max=1000,
                    closed="lower",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(0, 1000, "lower"),
            actions=_make_is_between_action(0, 1000, "lower"),
            dtypes=NUMERIC_DTYPES,
            name="range_0_1000_lower",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=None,
                    max=0,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(None, 0, "both"),
            actions=_make_is_between_action(None, 0, "both"),
            dtypes=NUMERIC_DTYPES,
            name="range_none_0_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=0,
                    max=None,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(0, None, "both"),
            actions=_make_is_between_action(0, None, "both"),
            dtypes=NUMERIC_DTYPES,
            name="range_0_none_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min=-1000,
                    max=0,
                    closed="upper",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(-1000, 0, "upper"),
            actions=_make_is_between_action(-1000, 0, "upper"),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="range_-1000_0_upper",
        ),
    ],
    IsNotBetween: [
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=-100,
                    max=100,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(-100, 100, "both"),
            actions=_make_is_not_between_action(-100, 100, "both"),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="range_-100_100_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=0,
                    max=1000,
                    closed="lower",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(0, 1000, "lower"),
            actions=_make_is_not_between_action(0, 1000, "lower"),
            dtypes=NUMERIC_DTYPES,
            name="range_0_1000_lower",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=None,
                    max=0,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(None, 0, "both"),
            actions=_make_is_not_between_action(None, 0, "both"),
            dtypes=NUMERIC_DTYPES,
            name="range_none_0_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=0,
                    max=None,
                    closed="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(0, None, "both"),
            actions=_make_is_not_between_action(0, None, "both"),
            dtypes=NUMERIC_DTYPES,
            name="range_0_none_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min=-1000,
                    max=0,
                    closed="upper",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(-1000, 0, "upper"),
            actions=_make_is_not_between_action(-1000, 0, "upper"),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="range_-1000_0_upper",
        ),
    ],
    IsIn: [
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({0, 1000}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({0, 1000}),
            actions=_make_is_in_action({0, 1000}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="values_0_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({None, 0}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({None, 0}),
            actions=_make_is_in_action({None, 0}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="none_and_0",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({-1000}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({-1000}),
            actions=_make_is_in_action({-1000}),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="negative_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({None, 1000}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({None, 1000}),
            actions=_make_is_in_action({None, 1000}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="none_and_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({NaN, 1000}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({NaN, 1000}),
            actions=_make_is_in_action({NaN, 1000}),
            dtypes=NAN_CAPABLE_DTYPES,
            name="nan_and_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsIn(
                    column_names=("value", "value_is_in"),
                    tags="is_in",
                    values=frozenset({None, NaN, 0}),
                ),
            ],
            method=DataQualityEngine.is_in,
            checker=_make_is_in_checker({None, NaN, 0}),
            actions=_make_is_in_action({None, NaN, 0}),
            dtypes=NAN_CAPABLE_DTYPES,
            name="none_nan_0",
        ),
    ],
    IsNotIn: [
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({0, 1000}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({0, 1000}),
            actions=_make_is_not_in_action({0, 1000}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="values_0_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({None, 0}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({None, 0}),
            actions=_make_is_not_in_action({None, 0}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="none_and_0",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({-1000}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({-1000}),
            actions=_make_is_not_in_action({-1000}),
            dtypes=SIGNED_NUMERIC_DTYPES,
            name="negative_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({None, 1000}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({None, 1000}),
            actions=_make_is_not_in_action({None, 1000}),
            dtypes=NULL_CAPABLE_DTYPES,
            name="none_and_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({NaN, 1000}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({NaN, 1000}),
            actions=_make_is_not_in_action({NaN, 1000}),
            dtypes=NAN_CAPABLE_DTYPES,
            name="nan_and_1000",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotIn(
                    column_names=("value", "value_is_not_in"),
                    tags="is_not_in",
                    values=frozenset({None, NaN, 0}),
                ),
            ],
            method=DataQualityEngine.is_not_in,
            checker=_make_is_not_in_checker({None, NaN, 0}),
            actions=_make_is_not_in_action({None, NaN, 0}),
            dtypes=NAN_CAPABLE_DTYPES,
            name="none_nan_0",
        ),
    ],
    Matches: [
        ClassifierConfiguration(
            classifiers=[
                Matches(
                    column_names=("value", "value_matches"),
                    tags="matches",
                    pattern=".*",
                ),
            ],
            method=DataQualityEngine.matches,
            checker=_make_matches_checker(".*"),
            actions=_make_matches_action(".*"),
            dtypes=STRING_DTYPES,
            name="all",
        ),
        ClassifierConfiguration(
            classifiers=[
                Matches(
                    column_names=("value", "value_matches"),
                    tags="matches",
                    pattern="^10",
                ),
            ],
            method=DataQualityEngine.matches,
            checker=_make_matches_checker("^10"),
            actions=_make_matches_action("^10"),
            dtypes=STRING_DTYPES,
            name="start10",
        ),
        ClassifierConfiguration(
            classifiers=[
                Matches(
                    column_names=("value", "value_matches"),
                    tags="matches",
                    pattern="^-10",
                ),
            ],
            method=DataQualityEngine.matches,
            checker=_make_matches_checker("^-10"),
            actions=_make_matches_action("^-10"),
            dtypes=STRING_DTYPES,
            name="start_minus10",
        ),
        ClassifierConfiguration(
            classifiers=[
                Matches(
                    column_names=("value", "value_matches"),
                    tags="matches",
                    pattern="^0$",
                ),
            ],
            method=DataQualityEngine.matches,
            checker=_make_matches_checker("^0$"),
            actions=_make_matches_action("^0$"),
            dtypes=STRING_DTYPES,
            name="exact0",
        ),
    ],
    DoesNotMatch: [
        ClassifierConfiguration(
            classifiers=[
                DoesNotMatch(
                    column_names=("value", "value_does_not_match"),
                    tags="does_not_match",
                    pattern=".*",
                ),
            ],
            method=DataQualityEngine.does_not_match,
            checker=_make_does_not_match_checker(".*"),
            actions=_make_does_not_match_action(".*"),
            dtypes=STRING_DTYPES,
            name="all",
        ),
        ClassifierConfiguration(
            classifiers=[
                DoesNotMatch(
                    column_names=("value", "value_does_not_match"),
                    tags="does_not_match",
                    pattern="^10",
                ),
            ],
            method=DataQualityEngine.does_not_match,
            checker=_make_does_not_match_checker("^10"),
            actions=_make_does_not_match_action("^10"),
            dtypes=STRING_DTYPES,
            name="start10",
        ),
        ClassifierConfiguration(
            classifiers=[
                DoesNotMatch(
                    column_names=("value", "value_does_not_match"),
                    tags="does_not_match",
                    pattern="^-10",
                ),
            ],
            method=DataQualityEngine.does_not_match,
            checker=_make_does_not_match_checker("^-10"),
            actions=_make_does_not_match_action("^-10"),
            dtypes=STRING_DTYPES,
            name="start_minus10",
        ),
        ClassifierConfiguration(
            classifiers=[
                DoesNotMatch(
                    column_names=("value", "value_does_not_match"),
                    tags="does_not_match",
                    pattern="^0$",
                ),
            ],
            method=DataQualityEngine.does_not_match,
            checker=_make_does_not_match_checker("^0$"),
            actions=_make_does_not_match_action("^0$"),
            dtypes=STRING_DTYPES,
            name="exact0",
        ),
    ],
    HasLength: [
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min=0,
                    max=1,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(0, 1),
            actions=_make_has_length_action(0, 1),
            dtypes=STRING_DTYPES,
            name="length_0_1",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min=5,
                    max=5,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(5, 5),
            actions=_make_has_length_action(5, 5),
            dtypes=STRING_DTYPES,
            name="length_5",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min=5,
                    max=6,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(5, 6),
            actions=_make_has_length_action(5, 6),
            dtypes=STRING_DTYPES,
            name="length_5_6",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min=0,
                    max=5,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(0, 5),
            actions=_make_has_length_action(0, 5),
            dtypes=STRING_DTYPES,
            name="length_0_5",
        ),
    ],
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
