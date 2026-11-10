#
# Copyright 2026 Tabsdata Inc.
#

"""A column of driver values built into its dtype, offline: every kind of
value the probed drivers hand back that Polars cannot build as it comes."""

from __future__ import annotations

import array
import datetime as dt
import ipaddress
import uuid
from collections import namedtuple
from decimal import Decimal
from types import SimpleNamespace

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.described.commons import values

pytestmark = pytest.mark.sql_common

UTC = dt.timezone.utc
PLUS_TWO = dt.timezone(dt.timedelta(hours=2))
IntervalYM = namedtuple("IntervalYM", "years months")


def _pg8000_interval(**units):
    """pg8000's interval: a field per unit, None for the units it lacks."""
    names = (
        "millennia centuries decades years months weeks days hours minutes"
        " seconds microseconds"
    ).split()
    return SimpleNamespace(**{name: units.get(name) for name in names})


@pytest.mark.parametrize(
    "value,dtype,expected",
    [
        (memoryview(b"\x01\x02"), pl.Binary(), b"\x01\x02"),
        (bytearray(b"\x01\x02"), pl.Binary(), b"\x01\x02"),
        (
            uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            pl.String(),
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        ),
        (ipaddress.ip_network("10.0.0.0/24"), pl.String(), "10.0.0.0/24"),
        (ipaddress.ip_address("::1"), pl.String(), "::1"),
        ({"a": [1, 2.5, "x"]}, pl.String(), '{"a": [1, 2.5, "x"]}'),
        ([1, "x", None], pl.String(), '[1, "x", null]'),
        (
            {"a": [Decimal("1"), Decimal("2.50"), Decimal("1E+2")]},
            pl.String(),
            '{"a": [1, 2.50, 100]}',
        ),
        ({"n": Decimal("NaN")}, pl.String(), '{"n": null}'),
        ({"d": dt.date(2026, 4, 19)}, pl.String(), '{"d": "2026-04-19"}'),
        ({"b": b"\x01"}, pl.String(), '{"b": "AQ=="}'),
        ({"text": "\x00quoted\x00"}, pl.String(), '{"text": "\\u0000quoted\\u0000"}'),
        ({"b", "a"}, pl.String(), "a,b"),
        (frozenset(), pl.String(), ""),
        (dt.time(6, 30, 24, tzinfo=PLUS_TWO), pl.String(), "06:30:24+02:00"),
        (Decimal("-1" + "0" * 40 + ".5"), pl.String(), "-1" + "0" * 40 + ".5"),
        (Decimal("1E+5"), pl.String(), "100000"),
        (7, pl.String(), "7"),
        (b"\x81", pl.UInt64(), 129),
        (b"\xff" * 8, pl.UInt64(), 2**64 - 1),
        (
            _pg8000_interval(months=1, days=2, hours=3),
            pl.Duration("us"),
            dt.timedelta(days=32, hours=3),
        ),
        (
            _pg8000_interval(years=1, months=2),
            pl.Duration("us"),
            dt.timedelta(days=425),
        ),
        (
            _pg8000_interval(seconds=5.5),
            pl.Duration("us"),
            dt.timedelta(seconds=5.5),
        ),
        (
            IntervalYM(1, 2),
            pl.Struct({"years": pl.Int64(), "months": pl.Int64()}),
            {"years": 1, "months": 2},
        ),
        (array.array("f", [1.0, 2.0]), pl.List(pl.Float32()), [1.0, 2.0]),
        ((1.0, 2.0), pl.List(pl.Float64()), [1.0, 2.0]),
    ],
)
def test_a_value_is_converted_to_what_its_dtype_holds(value, dtype, expected):
    built = values.build("c", [None, value], dtype)
    assert built.series.dtype == dtype
    assert built.series.to_list() == [None, expected]


@pytest.mark.parametrize(
    "dtype,values_,expected",
    [
        (pl.Int8(), [-128, 127], [-128, 127]),
        (pl.UInt64(), [2**64 - 1], [2**64 - 1]),
        (pl.Decimal(10, 2), [Decimal("-99999999.99")], [Decimal("-99999999.99")]),
        (pl.Decimal(10, 0), [7], [Decimal(7)]),
        (pl.Float32(), [Decimal("-1.5")], [-1.5]),
        (pl.Float64(), ["-1.5", "NaN"], [-1.5, float("nan")]),
        (pl.Int32(), [-9999900.0], [-9999900]),
        (pl.Boolean(), [True, False], [True, False]),
        (pl.Date(), [dt.date(2026, 4, 19)], [dt.date(2026, 4, 19)]),
        (pl.Null(), [None], [None]),
    ],
)
def test_a_value_that_fits_or_casts_keeps_its_value(dtype, values_, expected):
    built = values.build("c", [None, *values_], dtype)
    assert built.series.dtype == dtype
    assert built.series.to_list()[1:] == pytest.approx(expected, nan_ok=True)


def test_a_zoned_datetime_is_read_in_utc():
    value = dt.datetime(2026, 4, 19, 6, 30, tzinfo=PLUS_TWO)
    built = values.build("c", [value], pl.Datetime("us", "UTC"))
    assert built.series.to_list() == [dt.datetime(2026, 4, 19, 4, 30, tzinfo=UTC)]


def test_a_built_column_has_exactly_its_dtype_when_a_driver_zones_it():
    value = dt.datetime(2026, 4, 19, 6, 30, tzinfo=PLUS_TWO)
    built = values.build("c", [value], pl.Datetime("us"))
    assert built.series.dtype == pl.Datetime("us")


@pytest.mark.parametrize(
    "special", [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Inf")]
)
def test_a_decimal_that_is_not_finite_becomes_null(special):
    built = values.build("c", [Decimal("1.5"), special, None], pl.Decimal(10, 2))
    assert built.series.to_list() == [Decimal("1.50"), None, None]
    assert built.nulled == 1


def test_an_inferred_decimal_that_is_not_finite_becomes_null():
    built = values.build("c", [Decimal("1.5"), Decimal("NaN")], None)
    assert built.series.to_list() == [Decimal("1.5"), None]
    assert built.nulled == 1


def test_an_inferred_decimal_too_wide_for_a_decimal_column_becomes_text():
    wide = Decimal("-" + "9" * 39)
    built = values.build("c", [None, Decimal("1.5"), wide], None)
    assert built.series.dtype == pl.String
    assert built.series.to_list() == [None, "1.5", "-" + "9" * 39]
    assert built.wide


def test_a_value_the_dtype_cannot_hold_fails_the_build():
    with pytest.raises(pl.exceptions.InvalidOperationError):
        values.build("c", [Decimal("123456.78")], pl.Decimal(5, 2))


@pytest.mark.parametrize(
    "column,dtype",
    [
        ([1, 2.5], pl.Float64),
        ([Decimal("1.5"), Decimal("2.25")], pl.Decimal(38, 2)),
        ([None, None], pl.Null),
        ([dt.date(2026, 1, 1), dt.datetime(2026, 1, 1, 1)], pl.Datetime("us")),
        ([{"a": 1}, {"a": 2.5}], pl.Struct({"a": pl.Float64})),
        ([[1, 2], [3]], pl.List(pl.Int64)),
    ],
)
def test_an_undescribed_column_is_inferred_from_all_its_values(column, dtype):
    assert values.build("c", column, None).series.dtype == dtype


@pytest.mark.parametrize(
    "value,expected",
    [
        (memoryview(b"\x01"), b"\x01"),
        (uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), None),
        (ipaddress.ip_network("10.0.0.0/24"), "10.0.0.0/24"),
    ],
)
def test_an_undescribed_column_no_dtype_holds_is_converted(value, expected):
    built = values.build("c", [None, value], None)
    assert built.series.dtype != pl.Object
    assert built.series[1] == (expected if expected is not None else str(value))
