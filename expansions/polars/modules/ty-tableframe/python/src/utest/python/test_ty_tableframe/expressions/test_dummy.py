#
# Copyright 2025 Tabsdata Inc.
#

import logging

import polars as pl
import pytest

from tabsdata.expansions.tableframe.expressions import dummy_expr

logger = logging.getLogger(__name__)


def test_dummy_expr():
    lf = pl.LazyFrame({"input_column": ["1", "2", "3", "4", "5"] * 5})
    df = lf.collect()
    assert df.shape == (25, 1)
    lf = lf.with_columns(dummy_expr("input_column").alias("dummy_column"))
    df = lf.collect()
    assert df.shape == (25, 2)
    assert df.shape[0] == 25
    assert "dummy_column" in df.columns
    assert df.shape[1] >= 1
    assert df.schema["dummy_column"] == pl.String
    assert df.select(pl.col("dummy_column").n_unique()).item() == 1
    assert df["dummy_column"][0] == "dummy string"
    assert df["dummy_column"].to_list() == ["dummy string"] * 25


EVERY_DTYPE = {
    "i8": pl.Int8,
    "i16": pl.Int16,
    "i32": pl.Int32,
    "i64": pl.Int64,
    "i128": pl.Int128,
    "u8": pl.UInt8,
    "u16": pl.UInt16,
    "u32": pl.UInt32,
    "u64": pl.UInt64,
    "u128": pl.UInt128,
    "f16": pl.Float16,
    "f32": pl.Float32,
    "f64": pl.Float64,
    "decimal": pl.Decimal(10, 2),
    "date": pl.Date,
    "time": pl.Time,
    "datetime": pl.Datetime("us"),
    "datetime_utc": pl.Datetime("us", "UTC"),
    "duration": pl.Duration("us"),
    "boolean": pl.Boolean,
    "string": pl.String,
    "binary": pl.Binary,
    "categorical": pl.Categorical,
    "enum": pl.Enum(["1", "3"]),
    "list": pl.List(pl.Int64),
    "array": pl.Array(pl.Int64, 1),
    "struct": pl.Struct({"value": pl.Int64}),
    "null": pl.Null,
}


def _column(dtype) -> pl.Series:
    numbers = pl.Series([1, None, 3])
    if dtype in (pl.String, pl.Binary, pl.Categorical) or isinstance(dtype, pl.Enum):
        return numbers.cast(pl.String).cast(dtype)
    if isinstance(dtype, (pl.List, pl.Array)):
        return pl.Series([[1], None, [3]]).cast(dtype)
    if isinstance(dtype, pl.Struct):
        return pl.Series([{"value": 1}, None, {"value": 3}]).cast(dtype)
    if dtype == pl.Boolean:
        return pl.Series([True, None, False])
    if dtype == pl.Null:
        return pl.Series([None, None, None], dtype=pl.Null)
    return numbers.cast(dtype)


@pytest.mark.parametrize("dtype", EVERY_DTYPE.values(), ids=EVERY_DTYPE.keys())
def test_dummy_expr_takes_every_dtype(dtype):
    frame = pl.LazyFrame({"input_column": _column(dtype)})
    assert frame.collect_schema()["input_column"] == dtype
    df = frame.with_columns(dummy_expr("input_column").alias("dummy_column")).collect()
    assert df["dummy_column"].to_list() == ["dummy string"] * 3
