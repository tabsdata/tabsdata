#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pyarrow as pa
import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects import mysql

from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow


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
