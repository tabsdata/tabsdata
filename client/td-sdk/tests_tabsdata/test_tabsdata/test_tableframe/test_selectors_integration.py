#
# Copyright 2025 Tabs Data Inc.
#

import itertools
import logging
import random
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Callable, TypeAlias, Union

import polars as pl
import pytest
from polars import selectors as pl_selectors

import tabsdata as td
from tabsdata.extensions.tableframe.extension import SystemColumns
from tabsdata.tableframe import selectors as td_selectors

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import (
    _unwrap_table_frame,
    _wrap_polars_frame,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401
from .common import pretty_polars

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pretty_polars()

# pk == Library Package
pk: TypeAlias = Union[td.tableframe.selectors, pl.selectors]
# ft = Frame Type
ft: TypeAlias = Union[td.TableFrame, pl.LazyFrame]

DTYPES = [
    pl.Boolean,
    pl.Date,
    pl.Datetime,
    pl.Duration,
    pl.Float32,
    pl.Float64,
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.Int128,
    pl.Null,
    pl.String,
    pl.Time,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
]

DTYPES_COMBINATIONS = list(itertools.combinations(DTYPES, 2))

ASCII_DIGITS = [str(i) for i in range(10)]

JAPANESE_DIGITS = ["一", "二", "三", "四", "五", "六", "七", "八", "九", "十"]


SCHEMA = {
    "User": td.String,
    "Alias": td.String,
    "User Name": td.String,
    "User Surname": td.String,
    "布石": td.String,
    "中盤": td.String,
    "Â ü": td.String,
    "ð ý": td.String,
    "þ æ": td.String,
    "User0": td.String,
    "Alias0": td.String,
    "User0 Name1": td.String,
    "User0 Surname1": td.String,
    "布0石1": td.String,
    "中0盤1": td.String,
    "Â0 ü1": td.String,
    "ð0 ý1": td.String,
    "þ0 æ1": td.String,
    "Binary": pl.Binary,
    "Binary Data": pl.Binary,
    "Boolean": td.Boolean,
    "Boolean Data": td.Boolean,
    "Categorical": td.Categorical,
    "Categorical Data": td.Categorical,
    "Date": td.Date,
    "Date Data": td.Date,
    "Datetime": td.Datetime,
    "Datetime Data": td.Datetime,
    "Decimal": td.Decimal(precision=10, scale=3),
    "Decimal Data": td.Decimal(precision=10, scale=3),
    "01234": td.String,
    "56789": td.String,
    "一二三四五": td.Float64,
    "六七八九十": td.Float64,
    "Duration": td.Duration,
    "Duration Data": td.Duration,
    "Float32": td.Float32,
    "Float32 Data": td.Float32,
    "Float64": td.Float64,
    "Float64 Data": td.Float64,
    "Int8": td.Int8,
    "Int8 Data": td.Int8,
    "Int16": td.Int16,
    "Int16 Data": td.Int16,
    "Int32": td.Int32,
    "Int32 Data": td.Int32,
    "Int64": td.Int64,
    "Int64 Data": td.Int64,
    "Int128": td.Int128,
    "UInt8": td.UInt8,
    "UInt8 Data": td.UInt8,
    "UInt16": td.UInt16,
    "UInt16 Data": td.UInt16,
    "UInt32": td.UInt32,
    "UInt32 Data": td.UInt32,
    "UInt64": td.UInt64,
    "UInt64 Data": td.UInt64,
    "String": td.String,
    "String Data": td.String,
    "Time": td.Time,
    "Time Data": td.Time,
    "Object": pl.Object,
    "Object Data": pl.Object,
}


def generate() -> pl.LazyFrame:
    data = {
        "User": [
            "alice",
            "bob",
            "charlie",
            "diana",
            "eve",
            "frank",
            "grace",
            "henry",
        ],
        "Alias": [
            "ali",
            "bobby",
            "chuck",
            "dee",
            "evie",
            "frankie",
            "gracie",
            "hank",
        ],
        "User Name": [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Henry",
        ],
        "User Surname": [
            "Smith",
            "Jones",
            "Brown",
            "White",
            "Green",
            "Blue",
            "Red",
            "Black",
        ],
        "布石": [
            "開局",
            "攻撃",
            "守備",
            "中央",
            "側面",
            "後方",
            "前進",
            "撤退",
        ],
        "中盤": [
            "戦略",
            "戦術",
            "連携",
            "統制",
            "機動",
            "展開",
            "集結",
            "分散",
        ],
        "Â ü": [
            "café",
            "naïve",
            "résumé",
            "piña",
            "jalapeño",
            "mañana",
            "niño",
            "año",
        ],
        "ð ý": [
            "Þórr",
            "Ýmir",
            "Óðinn",
            "Æsir",
            "Ragnarök",
            "Miðgarðr",
            "Ásgarðr",
            "Helheim",
        ],
        "þ æ": [
            "Ægir",
            "Þrúðr",
            "Friðr",
            "Væringr",
            "Þjálfi",
            "Ægishjálmr",
            "Árheimr",
            "Víðarr",
        ],
        "User0": [
            "user_a",
            "user_b",
            "user_c",
            "user_d",
            "user_e",
            "user_f",
            "user_g",
            "user_h",
        ],
        "Alias0": [
            "alias_1",
            "alias_2",
            "alias_3",
            "alias_4",
            "alias_5",
            "alias_6",
            "alias_7",
            "alias_8",
        ],
        "User0 Name1": [
            "Alpha",
            "Beta",
            "Gamma",
            "Delta",
            "Epsilon",
            "Zeta",
            "Eta",
            "Theta",
        ],
        "User0 Surname1": [
            "Anderson",
            "Johnson",
            "Williams",
            "Brown",
            "Davis",
            "Miller",
            "Wilson",
            "Moore",
        ],
        "布0石1": [
            "先手",
            "後手",
            "中手",
            "左手",
            "右手",
            "上手",
            "下手",
            "名手",
        ],
        "中0盤1": [
            "序盤",
            "中盤",
            "終盤",
            "実戦",
            "研究",
            "定跡",
            "新手",
            "妙手",
        ],
        "Â0 ü1": [
            "crème",
            "François",
            "José",
            "André",
            "René",
            "Agnès",
            "Eugène",
            "Irène",
        ],
        "ð0 ý1": [
            "Björk",
            "Eyjólfur",
            "Guðrún",
            "Hákon",
            "Ívar",
            "Jónatan",
            "Kristín",
            "Lárus",
        ],
        "þ0 æ1": [
            "Þóra",
            "Ævar",
            "Guðný",
            "Árni",
            "Íris",
            "Ómar",
            "Úlfur",
            "Ýr",
        ],
        "Binary": [
            b"\x01\x02\x03",
            b"\x04\x05\x06",
            b"\x07\x08\x09",
            b"\x0a\x0b\x0c",
            b"\x0d\x0e\x0f",
            b"\x10\x11\x12",
            b"\x13\x14\x15",
            b"\x16\x17\x18",
        ],
        "Binary Data": [
            b"data1",
            b"data2",
            b"data3",
            b"data4",
            b"data5",
            b"data6",
            b"data7",
            b"data8",
        ],
        "Boolean": [
            True,
            False,
            True,
            False,
            True,
            False,
            True,
            False,
        ],
        "Boolean Data": [
            False,
            True,
            False,
            True,
            False,
            True,
            False,
            True,
        ],
        "Categorical": [
            "cat_a",
            "cat_b",
            "cat_c",
            "cat_a",
            "cat_b",
            "cat_c",
            "cat_a",
            "cat_b",
        ],
        "Categorical Data": [
            "type_x",
            "type_y",
            "type_z",
            "type_x",
            "type_y",
            "type_z",
            "type_x",
            "type_y",
        ],
        "Date": [
            date(2024, 1, 15),
            date(2024, 2, 20),
            date(2024, 3, 10),
            date(2024, 4, 5),
            date(2024, 5, 12),
            date(2024, 6, 18),
            date(2024, 7, 22),
            date(2024, 8, 8),
        ],
        "Date Data": [
            date(2023, 12, 1),
            date(2023, 11, 15),
            date(2023, 10, 20),
            date(2023, 9, 10),
            date(2023, 8, 5),
            date(2023, 7, 25),
            date(2023, 6, 18),
            date(2023, 5, 30),
        ],
        "Datetime": [
            datetime(2024, 1, 15, 10, 30, 0),
            datetime(2024, 2, 20, 14, 45, 0),
            datetime(2024, 3, 10, 9, 15, 0),
            datetime(2024, 4, 5, 16, 20, 0),
            datetime(2024, 5, 12, 11, 10, 0),
            datetime(2024, 6, 18, 13, 35, 0),
            datetime(2024, 7, 22, 8, 50, 0),
            datetime(2024, 8, 8, 15, 25, 0),
        ],
        "Datetime Data": [
            datetime(2023, 12, 1, 9, 0, 0),
            datetime(2023, 11, 15, 14, 30, 0),
            datetime(2023, 10, 20, 16, 45, 0),
            datetime(2023, 9, 10, 11, 15, 0),
            datetime(2023, 8, 5, 13, 20, 0),
            datetime(2023, 7, 25, 10, 40, 0),
            datetime(2023, 6, 18, 15, 55, 0),
            datetime(2023, 5, 30, 12, 10, 0),
        ],
        "Decimal": [
            Decimal("123.456"),
            Decimal("789.012"),
            Decimal("345.678"),
            Decimal("901.234"),
            Decimal("567.890"),
            Decimal("123.001"),
            Decimal("999.999"),
            Decimal("0.001"),
        ],
        "Decimal Data": [
            Decimal("19.990"),
            Decimal("29.500"),
            Decimal("15.250"),
            Decimal("45.750"),
            Decimal("32.100"),
            Decimal("28.850"),
            Decimal("21.400"),
            Decimal("38.900"),
        ],
        "01234": [
            "alpha",
            "beta",
            "gamma",
            "delta",
            "epsilon",
            "zeta",
            "eta",
            "theta",
        ],
        "56789": [
            "one",
            "two",
            "three",
            "four",
            "five",
            "six",
            "seven",
            "eight",
        ],
        "一二三四五": [
            1.1,
            2.2,
            3.3,
            4.4,
            5.5,
            6.6,
            7.7,
            8.8,
        ],
        "六七八九十": [
            10.1,
            20.2,
            30.3,
            40.4,
            50.5,
            60.6,
            70.7,
            80.8,
        ],
        "Duration": [
            timedelta(hours=1, minutes=30),
            timedelta(hours=2, minutes=15),
            timedelta(hours=0, minutes=45),
            timedelta(hours=3, minutes=0),
            timedelta(hours=1, minutes=15),
            timedelta(hours=2, minutes=30),
            timedelta(hours=0, minutes=30),
            timedelta(hours=4, minutes=45),
        ],
        "Duration Data": [
            timedelta(minutes=15, seconds=30),
            timedelta(minutes=25, seconds=45),
            timedelta(minutes=10, seconds=15),
            timedelta(minutes=35, seconds=0),
            timedelta(minutes=20, seconds=30),
            timedelta(minutes=30, seconds=15),
            timedelta(minutes=12, seconds=45),
            timedelta(minutes=28, seconds=20),
        ],
        "Float32": [1.5, 2.7, 3.9, 4.1, 5.3, 6.8, 7.2, 8.6],
        "Float32 Data": [10.5, 20.7, 30.9, 40.1, 50.3, 60.8, 70.2, 80.6],
        "Float64": [
            1.123456789,
            2.234567890,
            3.345678901,
            4.456789012,
            5.567890123,
            6.678901234,
            7.789012345,
            8.890123456,
        ],
        "Float64 Data": [
            11.987654321,
            22.876543210,
            33.765432109,
            44.654321098,
            55.543210987,
            66.432109876,
            77.321098765,
            88.210987654,
        ],
        "Int8": [
            -1,
            2,
            -3,
            -4,
            5,
            -6,
            7,
            -8,
        ],
        "Int8 Data": [
            -10,
            20,
            -30,
            40,
            -50,
            60,
            -70,
            80,
        ],
        "Int16": [
            -100,
            200,
            -300,
            400,
            -500,
            600,
            -700,
            800,
        ],
        "Int16 Data": [
            -1000,
            2000,
            -3000,
            4000,
            -5000,
            6000,
            -7000,
            8000,
        ],
        "Int32": [
            -10000,
            20000,
            -30000,
            40000,
            -50000,
            60000,
            -70000,
            80000,
        ],
        "Int32 Data": [
            -100000,
            200000,
            -300000,
            400000,
            -500000,
            600000,
            -700000,
            800000,
        ],
        "Int64": [
            -1000000,
            2000000,
            -3000000,
            4000000,
            -5000000,
            6000000,
            -7000000,
            8000000,
        ],
        "Int64 Data": [
            -10000000,
            20000000,
            -30000000,
            40000000,
            -50000000,
            60000000,
            -70000000,
            80000000,
        ],
        "Int128": [
            -123456789012345,
            234567890123456,
            -345678901234567,
            456789012345678,
            -567890123456789,
            678901234567890,
            -789012345678901,
            890123456789012,
        ],
        "UInt8": [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
        ],
        "UInt8 Data": [
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
        ],
        "UInt16": [
            100,
            200,
            300,
            400,
            500,
            600,
            700,
            800,
        ],
        "UInt16 Data": [
            1000,
            2000,
            3000,
            4000,
            5000,
            6000,
            7000,
            8000,
        ],
        "UInt32": [
            10000,
            20000,
            30000,
            40000,
            50000,
            60000,
            70000,
            80000,
        ],
        "UInt32 Data": [
            100000,
            200000,
            300000,
            400000,
            500000,
            600000,
            700000,
            800000,
        ],
        "UInt64": [
            1000000,
            2000000,
            3000000,
            4000000,
            5000000,
            6000000,
            7000000,
            8000000,
        ],
        "UInt64 Data": [
            10000000,
            20000000,
            30000000,
            40000000,
            50000000,
            60000000,
            70000000,
            80000000,
        ],
        "String": [
            "string1",
            "string2",
            "string3",
            "string4",
            "string5",
            "string6",
            "string7",
            "string8",
        ],
        "String Data": [
            "text_a",
            "text_b",
            "text_c",
            "text_d",
            "text_e",
            "text_f",
            "text_g",
            "text_h",
        ],
        "Time": [
            time(10, 30, 0),
            time(14, 45, 30),
            time(9, 15, 45),
            time(16, 20, 15),
            time(11, 10, 30),
            time(13, 35, 0),
            time(8, 50, 45),
            time(15, 25, 30),
        ],
        "Time Data": [
            time(12, 0, 0),
            time(12, 30, 0),
            time(13, 0, 0),
            time(12, 15, 0),
            time(12, 45, 0),
            time(13, 15, 0),
            time(12, 10, 0),
            time(12, 40, 0),
        ],
        "Object": pl.Series(
            "Object",
            [
                {"id": 1, "valid": True},
                {"id": 2, "valid": False},
                {"id": 3, "valid": True},
                {"id": 4, "valid": False},
                {"id": 5, "valid": True},
                {"id": 6, "valid": False},
                {"id": 7, "valid": True},
                {"id": 8, "valid": False},
            ],
            dtype=pl.Object,
        ),
        "Object Data": pl.Series(
            "Object Data",
            [
                (1.1, "A"),
                (2.2, "B"),
                (3.3, "C"),
                (4.4, "D"),
                (5.5, "E"),
                (6.6, "F"),
                (7.7, "G"),
                (8.8, "H"),
            ],
            dtype=pl.Object,
        ),
    }

    df = pl.DataFrame(data)
    cast_expressions = []
    for column_name, column_dtype in SCHEMA.items():
        cast_expressions.append(pl.col(column_name).cast(column_dtype))
    lf = df.with_columns(cast_expressions).lazy()
    return lf


RUNS = 64

POLARS_LAZY_FRAME = generate()


def log_frame(f: td.TableFrame | pl.LazyFrame):
    if not isinstance(f, pl.LazyFrame) and not isinstance(f, td.TableFrame):
        raise ValueError("Expects a polars.LazyFrame or a tabsdata.TableFrame")
    if isinstance(f, td.TableFrame):
        f = _unwrap_table_frame(f)
    logger.info(f.collect())


def eq_pf_tf(
    pf: pl.LazyFrame,
    tf: td.TableFrame,
):
    if not isinstance(pf, pl.LazyFrame):
        raise ValueError("Expected a polars.LazyFrame as first argument")
    if not isinstance(tf, td.TableFrame):
        raise ValueError("Expected a tabsdata.TableFrame as second argument")
    tf = _unwrap_table_frame(tf)

    return set(pf.columns) == (set(tf.columns))


def api_tester(
    fn: Callable[[pk, ft], ft],
    polars_frame: pl.LazyFrame = POLARS_LAZY_FRAME,
    check: bool = True,
) -> (pl.LazyFrame, td.TableFrame):
    t = _wrap_polars_frame(polars_frame)

    pf = fn(pl_selectors, polars_frame)
    tf = fn(td_selectors, t)
    eq = eq_pf_tf(pf, tf)

    if check:
        if not eq:
            logger.error("Failed:")
            logger.error("  LazyFrame:")
            log_frame(pf)
            logger.error("  TableFrame:")
            log_frame(tf)

        assert eq
    else:
        assert True
    return pf, tf


def apply_selector(library: pk, frame: ft, selector_fn, exclude: bool = False):
    selector = selector_fn(library)
    if exclude:
        if library == pl_selectors:
            return frame.select(library.exclude(selector))
        else:
            # noinspection PyProtectedMember
            return frame.select(library.exclude(selector._expr))
    else:
        return frame.select(selector)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_all(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.all(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("ascii_only", [True, False])
@pytest.mark.parametrize("ignore_spaces", [True, False])
def test_select_alpha(exclude, ascii_only, ignore_spaces):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.alpha(
                ascii_only=ascii_only,
                ignore_spaces=ignore_spaces,
            ),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("ascii_only", [True, False])
@pytest.mark.parametrize("ignore_spaces", [True, False])
def test_select_alphanumeric(exclude, ascii_only, ignore_spaces):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.alphanumeric(
                ascii_only=ascii_only,
                ignore_spaces=ignore_spaces,
            ),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_binary(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.binary(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_boolean(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.boolean(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_dtype(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.by_dtype(*s_dtypes),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    q_dtypes = random.randint(1, len(DTYPES))
    s_dtypes = random.sample(DTYPES, q_dtypes)

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_index(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.by_index(indices),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_indices = random.randint(1, len(all_columns))
    indices = sorted(random.sample(range(len(all_columns)), q_indices))
    api_tester(fn)

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_name(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.by_name(*selected_columns),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_columns = random.randint(1, len(all_columns))
    selected_columns = sorted(random.sample(all_columns, q_columns))

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_categorical(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.categorical(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_contains(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.contains(*substrings),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_columns = random.randint(1, len(all_columns))
    s_columns = random.sample(all_columns, q_columns)
    substrings = []
    for s_column in s_columns:
        if len(s_column) < 2:
            substrings.append(s_column)
        else:
            start = random.randint(0, len(s_column) - 2)
            substrings.append(s_column[start : start + 2])

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_date(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.date(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_datetime(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.datetime(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_decimal(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.decimal(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("digit", ASCII_DIGITS)
def test_select_digit_ascii(exclude, digit):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.digit(digit),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("digit", JAPANESE_DIGITS)
def test_select_digit_non_ascii(exclude, digit):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.digit(digit),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_duration(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.duration(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_ends_with(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.ends_with(*suffixes),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_columns = random.randint(1, len(all_columns))
    s_columns = random.sample(all_columns, q_columns)
    suffixes = []
    for s_column in s_columns:
        suffix = s_column[-2:] if len(s_column) >= 2 else s_column
        suffixes.append(suffix)

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_first(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.first(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_float(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.float(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_integer(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.integer(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_last(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.last(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_matches(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.matches(single_regex),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_columns = random.randint(1, len(all_columns))
    s_columns = random.sample(all_columns, q_columns)
    regex_fragments = []
    for col in s_columns:
        if len(col) >= 4:
            mid = len(col) // 2
            fragment = f"{re.escape(col[:2])}.*{re.escape(col[mid:mid+2])}"
        elif len(col) >= 2:
            fragment = f"{re.escape(col[0])}.{re.escape(col[1])}"
        else:
            fragment = re.escape(col)
        regex_fragments.append(fragment)

    single_regex = "|".join(regex_fragments)

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_numeric(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.numeric(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_object(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.object(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_signed_integer(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.signed_integer(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("run_id", range(RUNS))
def test_select_by_starts_with(exclude, run_id):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.starts_with(*prefixes),
            exclude,
        )

    # noinspection PyTypeChecker
    random.seed(run_id)
    all_columns = list(SCHEMA.keys())
    q_columns = random.randint(1, len(all_columns))
    s_columns = random.sample(all_columns, q_columns)

    prefixes = []
    for s_column in s_columns:
        prefix = s_column[:2] if len(s_column) >= 2 else s_column
        prefixes.append(prefix)

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_string(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.string(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_temporal(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.temporal(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_time(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.time(),
            exclude,
        )

    api_tester(fn)


@pytest.mark.parametrize("exclude", [False, True])
def test_select_unsigned_integer(exclude):
    def fn(library: pk, frame: ft):
        return apply_selector(
            library,
            frame,
            lambda lib: lib.unsigned_integer(),
            exclude,
        )

    api_tester(fn)
