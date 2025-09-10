#
# Copyright 2025 Tabs Data Inc.
#

import logging
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import polars as pl

import tabsdata as td
import tabsdata.tableframe as tdf
from tabsdata.tableframe.functions.col import Column

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_column_schema():
    columns = [
        Column("id", td.Int64),
        Column("name", td.String),
        Column("score", td.Float64),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"id": 1, "name": "Alice", "score": 95.5},
        {"id": 2, "name": "Bob", "score": 88.0},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_integer_types():
    columns = [
        Column("int8_col", td.Int8),
        Column("int16_col", td.Int16),
        Column("int32_col", td.Int32),
        Column("int64_col", td.Int64),
        Column("int128_col", td.Int128),
    ]
    schema = tdf.Schema(columns)
    data = [
        {
            "int8_col": 127,
            "int16_col": 32767,
            "int32_col": 2147483647,
            "int64_col": 9223372036854775807,
            "int128_col": 170141183460469231731687303715884105727,
        },
        {
            "int8_col": -128,
            "int16_col": -32768,
            "int32_col": -2147483648,
            "int64_col": -9223372036854775808,
            "int128_col": -170141183460469231731687303715884105728,
        },
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_unsigned_integer_types():
    columns = [
        Column("uint8_col", td.UInt8),
        Column("uint16_col", td.UInt16),
        Column("uint32_col", td.UInt32),
        Column("uint64_col", td.UInt64),
    ]
    schema = tdf.Schema(columns)
    data = [
        {
            "uint8_col": 255,
            "uint16_col": 65535,
            "uint32_col": 4294967295,
            "uint64_col": 18446744073709551615,
        },
        {"uint8_col": 0, "uint16_col": 0, "uint32_col": 0, "uint64_col": 0},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_float_types():
    columns = [
        Column("float32_col", td.Float32),
        Column("float64_col", td.Float64),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"float32_col": 3.14159, "float64_col": 2.718281828459045},
        {"float32_col": -1.23456, "float64_col": -9.876543210987654},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_boolean_and_string():
    columns = [
        Column("bool_col", td.Boolean),
        Column("string_col", td.String),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"bool_col": True, "string_col": "Hello World"},
        {"bool_col": False, "string_col": "Goodbye"},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_temporal_types():
    columns = [
        Column("date_col", td.Date()),
        Column("datetime_col", td.Datetime()),
        Column("time_col", td.Time()),
        Column("duration_col", td.Duration()),
    ]
    schema = tdf.Schema(columns)
    data = [
        {
            "date_col": date(2025, 1, 15),
            "datetime_col": datetime(2025, 1, 15, 10, 30, 45),
            "time_col": time(14, 30, 0),
            "duration_col": timedelta(days=1, hours=2, minutes=30),
        },
        {
            "date_col": date(2024, 12, 31),
            "datetime_col": datetime(2024, 12, 31, 23, 59, 59),
            "time_col": time(0, 0, 0),
            "duration_col": timedelta(seconds=3600),
        },
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_decimal_type():
    columns = [
        Column("decimal_col", td.Decimal(precision=10, scale=3)),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"decimal_col": Decimal("123.456")},
        {"decimal_col": Decimal("-789.012")},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    # Check that the schema column names and basic types match
    assert "decimal_col" in lf.schema
    assert lf.schema["decimal_col"].is_decimal()
    # Check that the scale matches (precision may be inferred as None by Polars)
    assert lf.schema["decimal_col"].scale == 3


def test_column_schema_categorical_type():
    columns = [
        Column("category_col", td.Categorical()),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"category_col": "Category A"},
        {"category_col": "Category B"},
        {"category_col": "Category A"},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_enum_type():
    columns = [
        Column("enum_col", td.Enum(["red", "green", "blue"])),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"enum_col": "red"},
        {"enum_col": "green"},
        {"enum_col": "blue"},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_null_type():
    columns = [
        Column("null_col", td.Null),
    ]
    schema = tdf.Schema(columns)
    data = [
        {"null_col": None},
        {"null_col": None},
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert lf.schema == schema


def test_column_schema_mixed_types_comprehensive():
    columns = [
        Column("id", td.Int64),
        Column("name", td.String),
        Column("active", td.Boolean),
        Column("score", td.Float64),
        Column("rating", td.Float32),
        Column("small_int", td.Int8),
        Column("medium_int", td.Int32),
        Column("big_uint", td.UInt64),
        Column("created_date", td.Date()),
        Column("updated_at", td.Datetime()),
        Column("start_time", td.Time()),
        Column("elapsed", td.Duration()),
        Column("price", td.Decimal(precision=10, scale=2)),
        Column("category", td.Categorical()),
        Column("status", td.Enum(["pending", "active", "inactive"])),
        Column("nullable_field", td.Null),
    ]
    schema = tdf.Schema(columns)
    data = [
        {
            "id": 1,
            "name": "Alice",
            "active": True,
            "score": 95.5,
            "rating": 4.8,
            "small_int": 127,
            "medium_int": 1000000,
            "big_uint": 18446744073709551615,
            "created_date": date(2025, 1, 1),
            "updated_at": datetime(2025, 1, 15, 10, 30, 0),
            "start_time": time(9, 0, 0),
            "elapsed": timedelta(hours=2, minutes=30),
            "price": Decimal("99.99"),
            "category": "Premium",
            "status": "active",
            "nullable_field": None,
        },
        {
            "id": 2,
            "name": "Bob",
            "active": False,
            "score": 88.0,
            "rating": 4.2,
            "small_int": -50,
            "medium_int": -500000,
            "big_uint": 0,
            "created_date": date(2024, 12, 15),
            "updated_at": datetime(2024, 12, 31, 23, 59, 59),
            "start_time": time(17, 30, 0),
            "elapsed": timedelta(minutes=45),
            "price": Decimal("49.99"),
            "category": "Standard",
            "status": "inactive",
            "nullable_field": None,
        },
    ]
    lf = pl.LazyFrame(data, schema=schema)
    assert len(lf.schema.names()) == len(schema.names())
    for col_name, expected_dtype in schema.items():
        assert col_name in lf.schema
        actual_dtype = lf.schema[col_name]
        if expected_dtype.is_decimal():
            assert actual_dtype.is_decimal()
            assert actual_dtype.scale == expected_dtype.scale
        else:
            assert actual_dtype == expected_dtype
