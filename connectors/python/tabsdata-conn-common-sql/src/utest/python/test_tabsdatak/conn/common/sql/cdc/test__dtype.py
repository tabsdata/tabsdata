#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal

import pyarrow as pa
import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects import mysql, postgresql

from tabsdatak.conn.common.sql.cdc._dtype import (
    sqlalchemy_to_arrow,
    to_text,
    value_converter,
)


@pytest.mark.parametrize(
    ("sa_type", "expected"),
    [
        (sa_types.SmallInteger(), pa.int16()),
        (sa_types.Integer(), pa.int32()),
        (sa_types.BigInteger(), pa.int64()),
        (mysql.TINYINT(), pa.int32()),
        (mysql.SMALLINT(), pa.int16()),
        (mysql.MEDIUMINT(), pa.int32()),
        (mysql.INTEGER(), pa.int32()),
        (mysql.BIGINT(), pa.int64()),
        (mysql.TINYINT(unsigned=True), pa.int64()),
        (mysql.SMALLINT(unsigned=True), pa.int32()),
        (mysql.MEDIUMINT(unsigned=True), pa.int64()),
        (mysql.INTEGER(unsigned=True), pa.int64()),
        (mysql.BIGINT(unsigned=True), pa.uint64()),
    ],
    ids=lambda value: repr(value),
)
def test_an_integer_maps_to_a_type_holding_its_whole_range(sa_type, expected):
    assert sqlalchemy_to_arrow(sa_type) == expected


@pytest.mark.parametrize(
    ("sa_type", "largest"),
    [
        (mysql.SMALLINT(unsigned=True), 65_535),
        (mysql.INTEGER(unsigned=True), 4_294_967_295),
        (mysql.BIGINT(unsigned=True), 18_446_744_073_709_551_615),
    ],
    ids=["smallint", "int", "bigint"],
)
def test_an_unsigned_integer_holds_its_largest_value(sa_type, largest):
    assert pa.array([largest], type=sqlalchemy_to_arrow(sa_type))[0].as_py() == largest


@pytest.mark.parametrize(
    ("sa_type", "expected"),
    [
        (sa_types.Time(), pa.time64("us")),
        (mysql.TIME(), pa.duration("us")),
        (mysql.TIME(fsp=6), pa.duration("us")),
    ],
    ids=["generic", "mysql", "mysql-fsp"],
)
def test_a_time_maps_to_what_its_database_means_by_it(sa_type, expected):
    assert sqlalchemy_to_arrow(sa_type) == expected


@pytest.mark.parametrize(
    ("sa_type", "expected"),
    [
        (sa_types.Float(precision=4), pa.float32()),
        (mysql.FLOAT(), pa.float64()),
        (mysql.FLOAT(precision=7, scale=4), pa.float64()),
    ],
    ids=["generic", "mysql", "mysql-precision"],
)
def test_a_float_maps_to_what_its_database_means_by_it(sa_type, expected):
    assert sqlalchemy_to_arrow(sa_type) == expected


def test_a_mysql_time_holds_its_whole_range_as_read():
    values = [
        timedelta(hours=9, minutes=30),
        timedelta(hours=838, minutes=59, seconds=59),
        -timedelta(hours=838, minutes=59, seconds=59),
        timedelta(microseconds=1),
        None,
    ]
    array = pa.array(values, type=sqlalchemy_to_arrow(mysql.TIME(fsp=6)))
    assert array.to_pylist() == values


@pytest.mark.parametrize(
    "sa_type",
    [
        sa_types.String(),
        sa_types.Text(),
        sa_types.Enum("a", "b"),
        mysql.ENUM("a", "b"),
        sa_types.Integer(),
        sa_types.Numeric(12, 2),
        sa_types.DateTime(),
        sa_types.LargeBinary(),
        mysql.TIME(),
    ],
    ids=lambda value: repr(value),
)
def test_a_column_whose_values_fit_its_type_needs_no_converter(sa_type):
    assert value_converter(sa_type) is None


@pytest.mark.parametrize(
    "sa_type",
    [
        sa_types.JSON(),
        postgresql.JSONB(),
        sa_types.Uuid(),
        sa_types.Interval(),
        postgresql.ARRAY(sa_types.Integer()),
        sa_types.Numeric(),
        postgresql.INET(),
        mysql.YEAR(),
        mysql.SET("a", "b"),
    ],
    ids=lambda value: repr(value),
)
def test_a_type_falling_back_to_text_converts_its_values_to_text(sa_type):
    assert value_converter(sa_type) is to_text


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("as is", "as is"),
        ({"k": [1, 2]}, '{"k":[1,2]}'),
        ([1, "two", None], '[1,"two",null]'),
        ({"b", "a"}, "a,b"),
        (b"\x01\xff", "01ff"),
        (
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "12345678-1234-5678-1234-567812345678",
        ),
        (Decimal("12345678901234567890.123456789"), "12345678901234567890.123456789"),
        (timedelta(days=1, hours=2), "1 day, 2:00:00"),
        (2026, "2026"),
    ],
    ids=lambda value: repr(value)[:30],
)
def test_to_text_keeps_json_as_json_and_spells_the_rest(value, expected):
    assert to_text(value) == expected
