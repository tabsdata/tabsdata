#
# Copyright 2025 Tabsdata Inc.
#

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable

import numpy as np
import polars as pl

import tabsdata as td
import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata.dataquality import (
    DataQuality,
    DoesNotMatch,
    HasLength,
    IsBetween,
    IsFalse,
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
    IsTrue,
    IsZero,
    Matches,
    Summary,
)
from tabsdata.dataquality._classifier import (
    Classifier,
    ExponentialScale,
    IdentityScale,
    IsNegativeOrZero,
    IsNotNullNorNan,
    IsNotZero,
    IsNullOrNan,
    LinearScale,
    LogarithmicScale,
    MonomialScale,
    ScaleCategorizer,
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

Q_BOOLEAN = Q_ZERO + Q_POSITIVE + Q_NEGATIVE + Q_NULL + Q_NaN

Q_CATEGORIZER = 512


def percentage(part: int | float, whole: int | float) -> float:
    if whole == 0:
        return 0
    return (part * 100) / whole


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
        return max(0, int(value))

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
    dataset: Callable[[type], td.TableFrame] | None = None


def _is_null_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_null" in df.columns
    assert "null" in tags
    assert "value_is_null" in tags["null"]
    assert (
        df.schema["value_is_null"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_null']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_not_null_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_not_null" in df.columns
    assert "not_null" in tags
    assert "value_is_not_null" in tags["not_null"]
    assert df.schema["value_is_not_null"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_null']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_true_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_true" in df.columns
    assert "true" in tags
    assert "value_is_true" in tags["true"]
    assert (
        df.schema["value_is_true"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_true']}"

    dtype = df.schema["value"]
    assert (
        dtype == pl.Boolean
    ), f"is_true checker should only be called for Boolean types; got {dtype}"

    true_count = df.filter(pl.col("value").is_not_null() & pl.col("value")).height
    false_count = df.filter(pl.col("value").is_not_null() & ~pl.col("value")).height
    nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_true") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_true") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_true") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_true") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_true FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_true TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_true NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_true NULL: dq={dq_null_count}, expected={null_count}"


def _is_false_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_false" in df.columns
    assert "false" in tags
    assert "value_is_false" in tags["false"]
    assert (
        df.schema["value_is_false"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_false']}"

    dtype = df.schema["value"]
    assert (
        dtype == pl.Boolean
    ), f"is_false checker should only be called for Boolean types; got {dtype}"

    false_count = df.filter(pl.col("value").is_not_null() & pl.col("value")).height
    true_count = df.filter(pl.col("value").is_not_null() & ~pl.col("value")).height
    nan_count = 0
    null_count = df.filter(pl.col("value").is_null()).height

    dq_false_count = df.filter(pl.col("value_is_false") == ByteSentinel.FALSE).height
    dq_true_count = df.filter(pl.col("value_is_false") == ByteSentinel.TRUE).height
    dq_nan_count = df.filter(pl.col("value_is_false") == ByteSentinel.NAN).height
    dq_null_count = df.filter(pl.col("value_is_false") == ByteSentinel.NULL).height

    assert (
        dq_false_count == false_count
    ), f"is_false FALSE: dq={dq_false_count}, expected={false_count}"
    assert (
        dq_true_count == true_count
    ), f"is_false TRUE: dq={dq_true_count}, expected={true_count}"
    assert (
        dq_nan_count == nan_count
    ), f"is_false NAN: dq={dq_nan_count}, expected={nan_count}"
    assert (
        dq_null_count == null_count
    ), f"is_false NULL: dq={dq_null_count}, expected={null_count}"


def _is_nan_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_nan" in df.columns
    assert "nan" in tags
    assert "value_is_nan" in tags["nan"]
    assert (
        df.schema["value_is_nan"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_nan']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]
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


def _is_not_nan_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_not_nan" in df.columns
    assert "not_nan" in tags
    assert "value_is_not_nan" in tags["not_nan"]
    assert (
        df.schema["value_is_not_nan"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_not_nan']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]
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


def _is_null_or_nan_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_null_or_nan" in df.columns
    assert "null_or_nan" in tags
    assert "value_is_null_or_nan" in tags["null_or_nan"]
    assert df.schema["value_is_null_or_nan"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_null_or_nan']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]
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


def _is_not_null_nor_nan_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_not_null_nor_nan" in df.columns
    assert "not_null_nor_nan" in tags
    assert "value_is_not_null_nor_nan" in tags["not_null_nor_nan"]
    assert df.schema["value_is_not_null_nor_nan"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_null_nor_nan']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]
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


def _is_zero_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_zero" in df.columns
    assert "zero" in tags
    assert "value_is_zero" in tags["zero"]
    assert (
        df.schema["value_is_zero"] == pl.UInt8
    ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_zero']}"

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_not_zero_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_not_zero" in df.columns
    assert "not_zero" in tags
    assert "value_is_not_zero" in tags["not_zero"]
    assert df.schema["value_is_not_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_not_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_positive_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_positive" in df.columns
    assert "positive" in tags
    assert "value_is_positive" in tags["positive"]
    assert df.schema["value_is_positive"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_positive']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_positive_or_zero_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_positive_or_zero" in df.columns
    assert "positive_or_zero" in tags
    assert "value_is_positive_or_zero" in tags["positive_or_zero"]
    assert df.schema["value_is_positive_or_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_positive_or_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_negative_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_negative" in df.columns
    assert "negative" in tags
    assert "value_is_negative" in tags["negative"]
    assert df.schema["value_is_negative"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_negative']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _is_negative_or_zero_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
    assert "value" in df.columns
    assert "value_is_negative_or_zero" in df.columns
    assert "negative_or_zero" in tags
    assert "value_is_negative_or_zero" in tags["negative_or_zero"]
    assert df.schema["value_is_negative_or_zero"] == pl.UInt8, (
        "Expected UInt8 dtype for DQ result column; got"
        f" {df.schema['value_is_negative_or_zero']}"
    )

    dtype = df.schema["value"]
    is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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
    min_val: Any, max_val: Any, closed_on: RangeClosedOnSpec = "both"
):
    def _is_between_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_is_between" in df.columns
        assert "is_between" in tags
        assert "value_is_between" in tags["is_between"]
        assert df.schema["value_is_between"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_between']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float16, pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        converted_min = converter(min_val) if min_val is not None else None
        converted_max = converter(max_val) if max_val is not None else None

        polars_closed = CLOSED_MAPPING[closed_on]

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
            if closed_on in ("both", "upper"):
                condition = pl.col("value").le(pl.lit(converted_max))
            else:
                condition = pl.col("value").lt(pl.lit(converted_max))
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
            if closed_on in ("both", "lower"):
                condition = pl.col("value").ge(pl.lit(converted_min))
            else:
                condition = pl.col("value").gt(pl.lit(converted_min))
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
                pl.lit(converted_min), pl.lit(converted_max), closed=polars_closed
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
    min_val: Any, max_val: Any, closed_on: RangeClosedOnSpec = "both"
):
    def _is_not_between_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_is_not_between" in df.columns
        assert "is_not_between" in tags
        assert "value_is_not_between" in tags["is_not_between"]
        assert df.schema["value_is_not_between"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_not_between']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float16, pl.Float32, pl.Float64]

        dtype_class = type(pl_dtype)
        converter = (
            DATASET_ROWS[dtype_class].converter
            if dtype_class in DATASET_ROWS
            else lambda value: value
        )
        converted_min = converter(min_val) if min_val is not None else None
        converted_max = converter(max_val) if max_val is not None else None

        polars_closed = CLOSED_MAPPING[closed_on]

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
            if closed_on in ("both", "upper"):
                condition = pl.col("value").le(pl.lit(converted_max))
            else:
                condition = pl.col("value").lt(pl.lit(converted_max))
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
            if closed_on in ("both", "lower"):
                condition = pl.col("value").ge(pl.lit(converted_min))
            else:
                condition = pl.col("value").gt(pl.lit(converted_min))
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
                pl.lit(converted_min), pl.lit(converted_max), closed=polars_closed
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
    def _is_in_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_is_in" in df.columns
        assert "is_in" in tags
        assert "value_is_in" in tags["is_in"]
        assert (
            df.schema["value_is_in"] == pl.UInt8
        ), f"Expected UInt8 dtype for DQ result column; got {df.schema['value_is_in']}"

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float16, pl.Float32, pl.Float64]

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
    def _is_not_in_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_is_not_in" in df.columns
        assert "is_not_in" in tags
        assert "value_is_not_in" in tags["is_not_in"]
        assert df.schema["value_is_not_in"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_is_not_in']}"
        )

        pl_dtype = df.schema["value"]
        is_float = pl_dtype in [pl.Float16, pl.Float32, pl.Float64]

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
    def _matches_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_matches" in df.columns
        assert "matches" in tags
        assert "value_matches" in tags["matches"]
        assert df.schema["value_matches"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_matches']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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
    def _does_not_match_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_does_not_match" in df.columns
        assert "does_not_match" in tags
        assert "value_does_not_match" in tags["does_not_match"]
        assert df.schema["value_does_not_match"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_does_not_match']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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
    def _has_length_checker(df: pl.DataFrame, tags: dict, _qrows: int = None):
        assert "value" in df.columns
        assert "value_has_length" in df.columns
        assert "has_length" in tags
        assert "value_has_length" in tags["has_length"]
        assert df.schema["value_has_length"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_has_length']}"
        )

        dtype = df.schema["value"]
        is_float = dtype in [pl.Float16, pl.Float32, pl.Float64]

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


def _make_categorizer_checker(scale):
    scale_tag_map = {
        IdentityScale: "identity",
        LinearScale: "linear",
        MonomialScale: "monomial",
        LogarithmicScale: "logarithmic",
        ExponentialScale: "exponential",
    }
    expected_tag = scale_tag_map.get(type(scale))

    def _categorizer_checker(df: pl.DataFrame, tags: dict, qrows: int = None):
        assert qrows, "Number of original rows is mandatory for categorizer tests."

        assert len(df) >= Q_CATEGORIZER, (
            f"Expected {qrows} rows to be at least {Q_CATEGORIZER}, "
            f"but got {len(df)} instead. Rows were lost during processing."
        )

        assert len(df) == qrows, (
            f"Expected {qrows} rows (same as input dataset), "
            f"but got {len(df)} instead. Rows were lost during processing."
        )

        assert "value" in df.columns
        assert "value_category" in df.columns
        assert expected_tag in tags, (
            f"Expected tag '{expected_tag}' not found in tags. "
            f"Available tags: {list(tags.keys())}"
        )
        assert "value_category" in tags[expected_tag], (
            f"Column 'value_category' not found in tags['{expected_tag}']. "
            f"Available columns: {list(tags[expected_tag])}"
        )
        assert df.schema["value_category"] == pl.UInt8, (
            "Expected UInt8 dtype for DQ result column; got"
            f" {df.schema['value_category']}"
        )

        minimum, maximum = scale.scale_range
        dtype = df.schema["value"]

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

        def get_expected_bin(row_value):
            if row_value is None or (
                dtype in [pl.Float16, pl.Float32, pl.Float64] and math.isnan(row_value)
            ):
                return None
            if row_value < minimum:
                return ByteSentinel.UNDERFLOW
            if row_value > maximum:
                return ByteSentinel.OVERFLOW

            if scale.use_bin_zero and row_value == minimum:
                return 0

            for i, cut in enumerate(cuts, start=1):
                if row_value <= cut:
                    return i

            return scale.bins

        null_mask = pl.col("value").is_null()
        null_count = df.filter(null_mask).height
        dq_null_count = df.filter(pl.col("value_category") == ByteSentinel.NULL).height
        assert (
            dq_null_count == null_count
        ), f"NULL: dq={dq_null_count}, expected={null_count}"

        if dtype in [pl.Float16, pl.Float32, pl.Float64]:
            nan_mask = pl.col("value").is_nan()
            nan_count = df.filter(nan_mask).height
            dq_nan_count = df.filter(
                pl.col("value_category") == ByteSentinel.NAN
            ).height
            assert (
                dq_nan_count == nan_count
            ), f"NAN: dq={dq_nan_count}, expected={nan_count}"

        underflow_mask = pl.col("value").is_not_null() & (pl.col("value") < minimum)
        if dtype in [pl.Float16, pl.Float32, pl.Float64]:
            underflow_mask = underflow_mask & pl.col("value").is_not_nan()
        underflow_count = df.filter(underflow_mask).height
        dq_underflow_count = df.filter(
            pl.col("value_category") == ByteSentinel.UNDERFLOW
        ).height
        assert (
            dq_underflow_count == underflow_count
        ), f"UNDERFLOW: dq={dq_underflow_count}, expected={underflow_count}"

        overflow_mask = pl.col("value").is_not_null() & (pl.col("value") > maximum)
        if dtype in [pl.Float16, pl.Float32, pl.Float64]:
            overflow_mask = overflow_mask & pl.col("value").is_not_nan()
        overflow_count = df.filter(overflow_mask).height
        dq_overflow_count = df.filter(
            pl.col("value_category") == ByteSentinel.OVERFLOW
        ).height
        assert (
            dq_overflow_count == overflow_count
        ), f"OVERFLOW: dq={dq_overflow_count}, expected={overflow_count}"

        valid_mask = (
            pl.col("value").is_not_null()
            & (pl.col("value") >= minimum)
            & (pl.col("value") <= maximum)
        )
        if dtype in [pl.Float16, pl.Float32, pl.Float64]:
            valid_mask = valid_mask & pl.col("value").is_not_nan()

        valid_df = df.filter(valid_mask)

        for row in valid_df.iter_rows(named=True):
            value = row["value"]
            actual_bin = row["value_category"]
            expected_bin = get_expected_bin(value)
            assert actual_bin == expected_bin, (
                f"Value {value} assigned to bin {actual_bin}, "
                f"but expected bin {expected_bin} based on scale {type(scale).__name__}"
            )

    return _categorizer_checker


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


def _is_true_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsTrue(
            column_names=("value", "value_is_true"),
            tags="true",
        ),
        operators=Summary(),
    )


def _is_false_action(
    table: str = "table00",
    _dtype: type = None,
) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=IsFalse(
            column_names=("value", "value_is_false"),
            tags="false",
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
    min_val: Any, max_val: Any, closed_on: RangeClosedOnSpec = "both"
):
    def _is_between_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:
            converter = DATASET_ROWS[dtype].converter

            is_uint = dtype in (
                td_dtypes.UInt8,
                td_dtypes.UInt16,
                td_dtypes.UInt32,
                td_dtypes.UInt64,
            )

            if is_uint:
                if min_val is not None:
                    if min_val == NaN:
                        converted_min = None
                    elif isinstance(min_val, (int, float)):
                        converted_min = int(min_val)
                    else:
                        converted_min = converter(min_val)
                else:
                    converted_min = None

                if max_val is not None:
                    if max_val == NaN:
                        converted_max = None
                    elif isinstance(max_val, (int, float)):
                        converted_max = int(max_val)
                    else:
                        converted_max = converter(max_val)
                else:
                    converted_max = None
            else:
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
                    min_val=converted_min,
                    max_val=converted_max,
                    closed_on=closed_on,
                ),
            ],
            operators=Summary(),
        )

    return _is_between_action


def _make_is_not_between_action(
    min_val: Any, max_val: Any, closed_on: RangeClosedOnSpec = "both"
):
    def _is_not_between_action(
        table: str = "table00",
        dtype: type = None,
    ) -> DataQuality:
        if dtype is not None and dtype in DATASET_ROWS:

            converter = DATASET_ROWS[dtype].converter

            is_uint = dtype in (
                td_dtypes.UInt8,
                td_dtypes.UInt16,
                td_dtypes.UInt32,
                td_dtypes.UInt64,
            )

            if is_uint:
                if min_val is not None:
                    if min_val == NaN:
                        converted_min = None
                    elif isinstance(min_val, (int, float)):
                        converted_min = int(min_val)
                    else:
                        converted_min = converter(min_val)
                else:
                    converted_min = None

                if max_val is not None:
                    if max_val == NaN:
                        converted_max = None
                    elif isinstance(max_val, (int, float)):
                        converted_max = int(max_val)
                    else:
                        converted_max = converter(max_val)
                else:
                    converted_max = None
            else:
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
                    min_val=converted_min,
                    max_val=converted_max,
                    closed_on=closed_on,
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

            is_uint = dtype in (
                td_dtypes.UInt8,
                td_dtypes.UInt16,
                td_dtypes.UInt32,
                td_dtypes.UInt64,
            )

            converted_values = set()
            for value in values_set:
                if value is None:
                    converted_values.add(None)
                elif isinstance(value, float) and math.isnan(value):
                    converted_values.add(value)
                elif is_uint and value == NaN:
                    converted_values.add(None)
                elif is_uint and isinstance(value, (int, float)):
                    converted_values.add(int(value))
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

            is_uint = dtype in (
                td_dtypes.UInt8,
                td_dtypes.UInt16,
                td_dtypes.UInt32,
                td_dtypes.UInt64,
            )

            converted_values = set()
            for value in values_set:
                if value is None:
                    converted_values.add(None)
                elif isinstance(value, float) and math.isnan(value):
                    converted_values.add(value)
                elif is_uint and value == NaN:
                    converted_values.add(None)
                elif is_uint and isinstance(value, (int, float)):
                    converted_values.add(int(value))
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
                    min_len=min_length,
                    max_len=max_length,
                ),
            ],
            operators=Summary(),
        )

    return _has_length_action


def _make_categorizer_action(scale):
    scale_tag_map = {
        IdentityScale: "identity",
        LinearScale: "linear",
        MonomialScale: "monomial",
        LogarithmicScale: "logarithmic",
        ExponentialScale: "exponential",
    }
    tag = scale_tag_map.get(type(scale))

    def _categorizer_action(
        table: str = "table00",
        _dtype: type = None,
    ) -> DataQuality:
        return DataQuality(
            table=table,
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=scale,
                    tags=tag,
                )
            ],
            operators=Summary(),
        )

    return _categorizer_action


def _make_categorizer_dataset(scale):
    def _categorizer_dataset(dtype: type) -> td.TableFrame:
        return categorical_dataset(scale, dtype)

    return _categorizer_dataset


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
    td.Float16: DataShape(
        Q_ZERO,
        Q_POSITIVE,
        Q_NEGATIVE,
        Q_NULL,
        Q_NaN,
        DataShape.to_float,
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
            dtypes=list(IsNull.supported_dtypes()),
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
            dtypes=list(IsNotNull.supported_dtypes()),
        ),
    ],
    IsTrue: [
        ClassifierConfiguration(
            classifiers=[
                IsTrue(
                    column_names=("value", "value_is_true"),
                    tags="true",
                )
            ],
            method=DataQualityEngine.is_true,
            checker=_is_true_checker,
            actions=_is_true_action,
            dtypes=list(IsTrue.supported_dtypes()),
        ),
    ],
    IsFalse: [
        ClassifierConfiguration(
            classifiers=[
                IsFalse(
                    column_names=("value", "value_is_false"),
                    tags="false",
                )
            ],
            method=DataQualityEngine.is_false,
            checker=_is_false_checker,
            actions=_is_false_action,
            dtypes=list(IsFalse.supported_dtypes()),
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
            dtypes=list(IsNan.supported_dtypes()),
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
            dtypes=list(IsNotNan.supported_dtypes()),
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
            dtypes=list(IsNullOrNan.supported_dtypes()),
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
            dtypes=list(IsNotNullNorNan.supported_dtypes()),
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
            dtypes=list(IsZero.supported_dtypes()),
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
            dtypes=list(IsNotZero.supported_dtypes()),
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
            dtypes=list(IsPositive.supported_dtypes()),
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
            dtypes=list(IsPositiveOrZero.supported_dtypes()),
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
            dtypes=list(IsNegative.supported_dtypes()),
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
            dtypes=list(IsNegativeOrZero.supported_dtypes()),
        ),
    ],
    IsBetween: [
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min_val=-100,
                    max_val=100,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(-100, 100, "both"),
            actions=_make_is_between_action(-100, 100, "both"),
            dtypes=list(IsBetween.supported_dtypes()),
            name="range_-100_100_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min_val=0,
                    max_val=1000,
                    closed_on="lower",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(0, 1000, "lower"),
            actions=_make_is_between_action(0, 1000, "lower"),
            dtypes=list(IsBetween.supported_dtypes()),
            name="range_0_1000_lower",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min_val=None,
                    max_val=0,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(None, 0, "both"),
            actions=_make_is_between_action(None, 0, "both"),
            dtypes=list(IsBetween.supported_dtypes()),
            name="range_none_0_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min_val=0,
                    max_val=None,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(0, None, "both"),
            actions=_make_is_between_action(0, None, "both"),
            dtypes=list(IsBetween.supported_dtypes()),
            name="range_0_none_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsBetween(
                    column_names=("value", "value_is_between"),
                    tags="is_between",
                    min_val=-1000,
                    max_val=0,
                    closed_on="upper",
                ),
            ],
            method=DataQualityEngine.is_between,
            checker=_make_is_between_checker(-1000, 0, "upper"),
            actions=_make_is_between_action(-1000, 0, "upper"),
            dtypes=list(IsBetween.supported_dtypes()),
            name="range_-1000_0_upper",
        ),
    ],
    IsNotBetween: [
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min_val=-100,
                    max_val=100,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(-100, 100, "both"),
            actions=_make_is_not_between_action(-100, 100, "both"),
            dtypes=list(IsNotBetween.supported_dtypes()),
            name="range_-100_100_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min_val=0,
                    max_val=1000,
                    closed_on="lower",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(0, 1000, "lower"),
            actions=_make_is_not_between_action(0, 1000, "lower"),
            dtypes=list(IsNotBetween.supported_dtypes()),
            name="range_0_1000_lower",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min_val=None,
                    max_val=0,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(None, 0, "both"),
            actions=_make_is_not_between_action(None, 0, "both"),
            dtypes=list(IsNotBetween.supported_dtypes()),
            name="range_none_0_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min_val=0,
                    max_val=None,
                    closed_on="both",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(0, None, "both"),
            actions=_make_is_not_between_action(0, None, "both"),
            dtypes=list(IsNotBetween.supported_dtypes()),
            name="range_0_none_both",
        ),
        ClassifierConfiguration(
            classifiers=[
                IsNotBetween(
                    column_names=("value", "value_is_not_between"),
                    tags="is_not_between",
                    min_val=-1000,
                    max_val=0,
                    closed_on="upper",
                ),
            ],
            method=DataQualityEngine.is_not_between,
            checker=_make_is_not_between_checker(-1000, 0, "upper"),
            actions=_make_is_not_between_action(-1000, 0, "upper"),
            dtypes=list(IsNotBetween.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(IsNotIn.supported_dtypes()),
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
            dtypes=list(Matches.supported_dtypes()),
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
            dtypes=list(Matches.supported_dtypes()),
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
            dtypes=list(Matches.supported_dtypes()),
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
            dtypes=list(Matches.supported_dtypes()),
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
            dtypes=list(DoesNotMatch.supported_dtypes()),
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
            dtypes=list(DoesNotMatch.supported_dtypes()),
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
            dtypes=list(DoesNotMatch.supported_dtypes()),
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
            dtypes=list(DoesNotMatch.supported_dtypes()),
            name="exact0",
        ),
    ],
    HasLength: [
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min_len=0,
                    max_len=1,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(0, 1),
            actions=_make_has_length_action(0, 1),
            dtypes=list(HasLength.supported_dtypes()),
            name="length_0_1",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min_len=5,
                    max_len=5,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(5, 5),
            actions=_make_has_length_action(5, 5),
            dtypes=list(HasLength.supported_dtypes()),
            name="length_5",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min_len=5,
                    max_len=6,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(5, 6),
            actions=_make_has_length_action(5, 6),
            dtypes=list(HasLength.supported_dtypes()),
            name="length_5_6",
        ),
        ClassifierConfiguration(
            classifiers=[
                HasLength(
                    column_names=("value", "value_has_length"),
                    tags="has_length",
                    min_len=0,
                    max_len=5,
                ),
            ],
            method=DataQualityEngine.has_length,
            checker=_make_has_length_checker(0, 5),
            actions=_make_has_length_action(0, 5),
            dtypes=list(HasLength.supported_dtypes()),
            name="length_0_5",
        ),
    ],
    ScaleCategorizer: [
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=100,
                        bins=10,
                        use_bin_zero=False,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_100_10_with_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=100,
                        bins=10,
                        use_bin_zero=True,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_100_10_no_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=100,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=10,
                        bins=5,
                        use_bin_zero=False,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_10_5_with_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=10,
                        bins=5,
                        use_bin_zero=True,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_10_5_no_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=10,
                    bins=5,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=-50,
                        max_val=50,
                        bins=20,
                        use_bin_zero=False,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_neg50_50_20_with_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=-50,
                        max_val=50,
                        bins=20,
                        use_bin_zero=True,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_neg50_50_20_no_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=-50,
                    max_val=50,
                    bins=20,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=1,
                        bins=100,
                        use_bin_zero=False,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_1_100_with_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LinearScale(
                        min_val=0,
                        max_val=1,
                        bins=100,
                        use_bin_zero=True,
                    ),
                    tags="linear",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="linear_0_1_100_no_min",
            dataset=_make_categorizer_dataset(
                LinearScale(
                    min_val=0,
                    max_val=1,
                    bins=100,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=0,
                        max_val=10,
                        use_bin_zero=False,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_0_10_with_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=0,
                        max_val=10,
                        use_bin_zero=True,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_0_10_no_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=0,
                    max_val=10,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=1,
                        max_val=20,
                        use_bin_zero=False,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_1_20_with_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=1,
                        max_val=20,
                        use_bin_zero=True,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_1_20_no_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=1,
                    max_val=20,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=0,
                        max_val=100,
                        use_bin_zero=False,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_0_100_with_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=IdentityScale(
                        min_val=0,
                        max_val=100,
                        use_bin_zero=True,
                    ),
                    tags="identity",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="identity_0_100_no_min",
            dataset=_make_categorizer_dataset(
                IdentityScale(
                    min_val=0,
                    max_val=100,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=1,
                        max_val=100,
                        power=2.0,
                        bins=10,
                        use_bin_zero=False,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_quadratic_1_100_10_with_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=1,
                        max_val=100,
                        power=2.0,
                        bins=10,
                        use_bin_zero=True,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_quadratic_1_100_10_no_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=1,
                    max_val=100,
                    power=2.0,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=0,
                        max_val=27,
                        power=3.0,
                        bins=9,
                        use_bin_zero=False,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_cubic_0_27_9_with_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=0,
                        max_val=27,
                        power=3.0,
                        bins=9,
                        use_bin_zero=True,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_cubic_0_27_9_no_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=0,
                    max_val=27,
                    power=3.0,
                    bins=9,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=1,
                        max_val=1000,
                        power=2.0,
                        bins=30,
                        use_bin_zero=False,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_quadratic_1_1000_30_with_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=MonomialScale(
                        min_val=1,
                        max_val=1000,
                        power=2.0,
                        bins=30,
                        use_bin_zero=True,
                    ),
                    tags="monomial",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="monomial_quadratic_1_1000_30_no_min",
            dataset=_make_categorizer_dataset(
                MonomialScale(
                    min_val=1,
                    max_val=1000,
                    power=2.0,
                    bins=30,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=0,
                        max_val=6,
                        base=10.0,
                        bins=6,
                        use_bin_zero=False,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base10_0_6_6_with_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=0,
                        max_val=6,
                        base=10.0,
                        bins=6,
                        use_bin_zero=True,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base10_0_6_6_no_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=0,
                    max_val=6,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=1,
                        max_val=4,
                        base=2.0,
                        bins=4,
                        use_bin_zero=False,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base2_1_4_4_with_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=1,
                        max_val=4,
                        base=2.0,
                        bins=4,
                        use_bin_zero=True,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base2_1_4_4_no_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=1,
                    max_val=4,
                    base=2.0,
                    bins=4,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=0,
                        max_val=3,
                        base=math.e,
                        bins=10,
                        use_bin_zero=False,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base_e_0_3_10_with_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=LogarithmicScale(
                        min_val=0,
                        max_val=3,
                        base=math.e,
                        bins=10,
                        use_bin_zero=True,
                    ),
                    tags="logarithmic",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="logarithmic_base_e_0_3_10_no_min",
            dataset=_make_categorizer_dataset(
                LogarithmicScale(
                    min_val=0,
                    max_val=3,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000000,
                        base=10.0,
                        bins=6,
                        use_bin_zero=False,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base10_1_1M_6_with_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000000,
                        base=10.0,
                        bins=6,
                        use_bin_zero=True,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base10_1_1M_6_no_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000000,
                    base=10.0,
                    bins=6,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000,
                        base=10.0,
                        bins=3,
                        use_bin_zero=False,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base10_1_1000_3_with_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000,
                        base=10.0,
                        bins=3,
                        use_bin_zero=True,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base10_1_1000_3_no_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=10.0,
                    bins=3,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=2,
                        max_val=256,
                        base=2.0,
                        bins=8,
                        use_bin_zero=False,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base2_2_256_8_with_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=2,
                        max_val=256,
                        base=2.0,
                        bins=8,
                        use_bin_zero=True,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base2_2_256_8_no_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=2,
                    max_val=256,
                    base=2.0,
                    bins=8,
                    use_bin_zero=True,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000,
                        base=math.e,
                        bins=10,
                        use_bin_zero=False,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base_e_1_1000_10_with_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=False,
                )
            ),
        ),
        ClassifierConfiguration(
            classifiers=[
                ScaleCategorizer(
                    column_names=("value", "value_category"),
                    scale=ExponentialScale(
                        min_val=1,
                        max_val=1000,
                        base=math.e,
                        bins=10,
                        use_bin_zero=True,
                    ),
                    tags="exponential",
                ),
            ],
            method=DataQualityEngine.categorizer,
            checker=_make_categorizer_checker(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            actions=_make_categorizer_action(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
            dtypes=list(ScaleCategorizer.supported_dtypes()),
            name="exponential_base_e_1_1000_10_no_min",
            dataset=_make_categorizer_dataset(
                ExponentialScale(
                    min_val=1,
                    max_val=1000,
                    base=math.e,
                    bins=10,
                    use_bin_zero=True,
                )
            ),
        ),
    ],
}


def boolean_dataset(dtype) -> td.TableFrame:
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


def categorical_dataset(
    scale, dtype, sample_size: int = Q_CATEGORIZER
) -> td.TableFrame:
    minimum, maximum = scale.scale_range
    values = []
    values.extend(np.linspace(minimum, maximum, sample_size).tolist())

    values.extend([minimum, maximum])

    underflow = abs(maximum - minimum) * 0.1 + 1
    overflow = abs(maximum - minimum) * 0.1 + 1
    values.extend(
        [
            minimum - underflow,
            maximum + overflow,
        ]
    )

    if isinstance(scale, LinearScale):
        size = (maximum - minimum) / scale.bins
        for i in range(1, scale.bins):
            cut = minimum + i * size
            values.extend(
                [
                    cut - 0.001,
                    cut,
                    cut + 0.001,
                ]
            )
    elif isinstance(scale, IdentityScale):
        if maximum - minimum <= 100:
            values.extend(range(int(minimum), int(maximum) + 1))
        else:
            samples = np.linspace(minimum, maximum, 100, dtype=int)
            values.extend(samples.tolist())
    elif isinstance(scale, MonomialScale):
        lower = minimum ** (1.0 / scale.power)
        upper = maximum ** (1.0 / scale.power)
        samples = np.linspace(lower, upper, 50)
        samples = (samples**scale.power).tolist()
        values.extend(samples)
        if scale.power == 2.0:
            perfect_squares = [i**2 for i in range(1, 50) if minimum <= i**2 <= maximum]
            values.extend(perfect_squares)
        elif scale.power == 3.0:
            perfect_cubes = [i**3 for i in range(1, 20) if minimum <= i**3 <= maximum]
            values.extend(perfect_cubes)
    elif isinstance(scale, LogarithmicScale):
        for exponent in np.linspace(minimum, maximum, 50):
            values.append(scale.base**exponent)
        for i in range(int(minimum), int(maximum) + 1):
            values.append(scale.base**i)
    elif isinstance(scale, ExponentialScale):
        if minimum > 0:
            samples = np.logspace(
                math.log10(minimum) if scale.base == 10.0 else math.log(minimum),
                math.log10(maximum) if scale.base == 10.0 else math.log(maximum),
                100,
            )
            values.extend(samples.tolist())
        if scale.base == 10.0:
            powers = [10**i for i in range(10) if minimum <= 10**i <= maximum]
            values.extend(powers)
        elif scale.base == 2.0:
            powers = [2**i for i in range(20) if minimum <= 2**i <= maximum]
            values.extend(powers)

    epsilon = (maximum - minimum) / 1000
    values.extend(
        [
            minimum + epsilon,
            minimum + 2 * epsilon,
            minimum + 3 * epsilon,
            maximum - 3 * epsilon,
            maximum - 2 * epsilon,
            maximum - epsilon,
        ]
    )

    mid = (minimum + maximum) / 2
    quarter1 = minimum + (maximum - minimum) / 4
    quarter3 = minimum + 3 * (maximum - minimum) / 4
    values.extend(
        [
            quarter1 - epsilon,
            quarter1,
            quarter1 + epsilon,
            mid - epsilon,
            mid,
            mid + epsilon,
            quarter3 - epsilon,
            quarter3,
            quarter3 + epsilon,
        ]
    )

    values.append(None)

    pl_dtype = getattr(pl, dtype.__name__)
    if pl_dtype in [pl.Float16, pl.Float32, pl.Float64]:
        values.append(float("nan"))

    regular_values = [
        value
        for value in values
        if value is not None and not (isinstance(value, float) and math.isnan(value))
    ]
    special_values = [
        value
        for value in values
        if value is None or (isinstance(value, float) and math.isnan(value))
    ]

    if dtype in [td.Int8, td.Int16, td.Int32, td.Int64, td.Int128]:
        values = []
        for value in regular_values:
            try:
                ivalue = int(round(value))
                values.append(ivalue)
            except (ValueError, OverflowError):
                pass
        values.extend(special_values)
    else:
        values = regular_values + special_values

    data = {
        "id": list(range(len(values))),
        "value": values,
    }

    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )

    return tf
