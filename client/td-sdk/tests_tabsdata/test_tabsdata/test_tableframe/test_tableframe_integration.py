#
# Copyright 2025 Tabs Data Inc.
#

import logging
from typing import Callable, TypeAlias, Union

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import (
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
pk: TypeAlias = Union[td, pl]
# ft = Frame Type
ft: TypeAlias = Union[td.TableFrame, pl.LazyFrame]


POLARS_TABLE_FRAME_DUPLICATION_TIMES = 13

POLARS_TABLE_FRAME = pl.DataFrame(
    data={
        "u": [1, 0, 2, 3, 4, 5, 6, 7, 8, 9, None],
        "i": [1, 0, 2, 3, 4, -1, -2, -3, -4, -5, None],
        "f": [1.1, 0.0, 2.2, 3.3, 4.4, -1.1, -2.2, -3.3, -4.4, -5.5, None],
        "ff": [
            1.1,
            0.0,
            2.2,
            3.3,
            4.4,
            -1.1,
            -2.2,
            -3.3,
            float("inf"),
            float("nan"),
            None,
        ],
        "b": [True, False, True, False, True, None, True, False, True, True, None],
        "s": ["A ", " B", "AC", "D", "E", "F", "g", "h", "i", "j", None],
        "ss": ["A", "B", "A", "B", "B", "C", "C", "C", "D", "F", None],
        "d": [
            "2000-01-01",
            "2011-11-01",
            "2006-07-01",
            "2000-03-05",
            "2010-01-01",
            "2015-02-01",
            "2021-01-05",
            "2020-01-06",
            "2000-06-01",
            None,
            "1999-12-15",
        ],
        "dt": [
            "2000-01-01 00:00:10Z",
            "2011-11-01 19:10:10Z",
            "2006-07-01 21:40:21Z",
            "2000-03-05 00:10:10Z",
            "2010-01-01 17:20:00Z",
            "2015-02-01 22:05:01Z",
            "2021-01-05 01:03:11Z",
            "2020-01-06 15:30:00Z",
            "2000-06-01 20:50:13Z",
            "1999-12-15 00:00:00Z",
            "1999-12-15 00:00:00Z",
        ],
        "t": [
            "00:00:10",
            "19:10:10",
            "21:40:21",
            "00:10:10",
            "17:20:00",
            "22:05:01",
            "01:03:11",
            "15:30:00",
            "20:50:13",
            "00:00:00",
            "00:00:00",
        ],
        "si": ["1", "0", "2", "3", "4", "5", "6", "7", "8", "90", None],
    }
).lazy()

for i in range(POLARS_TABLE_FRAME_DUPLICATION_TIMES):
    POLARS_TABLE_FRAME = pl.concat([POLARS_TABLE_FRAME, POLARS_TABLE_FRAME])
POLARS_TABLE_FRAME = POLARS_TABLE_FRAME.collect().lazy()

POLARS_TABLE_FRAME_DATETIME = POLARS_TABLE_FRAME.with_columns(
    pl.col("d").str.to_date(),
    pl.col("dt").str.to_datetime(format="%Y-%m-%d %H:%M:%SZ", time_zone="UTC"),
)

POLARS_TABLE_FRAME_STRUCT = pl.DataFrame(
    {
        "id": [1, 2],
        "info": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": None},
        ],
        "status": ["active", "inactive"],
    }
).lazy()


def log_frame(f: td.TableFrame | pl.LazyFrame):
    if not isinstance(f, pl.LazyFrame) and not isinstance(f, td.TableFrame):
        raise ValueError("Expects a polars.LazyFrame or a tabsdata.TableFrame")
    if isinstance(f, td.TableFrame):
        f = _unwrap_table_frame(f)
    logger.info(f.collect())


def eq_pf_tf(
    pf: pl.LazyFrame,
    tf: td.TableFrame,
    sort: bool = True,
    rounded: bool = False,
    column: str = None,
):
    if not isinstance(pf, pl.LazyFrame):
        raise ValueError("Expects a polars.LazyFrame as 1st argument")
    if not isinstance(tf, td.TableFrame):
        raise ValueError("Expects a tabsdata.TableFrame as 2nd argument")
    if not isinstance(sort, bool):
        raise ValueError("Expects a bool as 3rd argument")
    tf = _unwrap_table_frame(tf)

    if rounded:
        pf = pf.with_columns(
            pl.col(column).cast(pl.Float64).fill_nan(0).fill_null(0).abs().round(3)
        )
        tf = tf.with_columns(
            pl.col(column).cast(pl.Float64).fill_nan(0).fill_null(0).abs().round(3)
        )

    if sort:
        pf = pf.sort(pl.first())
        tf = tf.sort(pl.first())
    return pf.collect().equals(tf.collect())


def api_tester(
    fn: Callable[[pk, ft], ft],
    polars_frame: pl.LazyFrame = POLARS_TABLE_FRAME,
    sort: bool = True,
    rounded: bool = False,
    column: str = None,
    check: bool = True,
):
    t = _wrap_polars_frame(polars_frame)

    pf = fn(pl, polars_frame)
    tf = fn(td, t)
    eq = eq_pf_tf(pf, tf, sort, rounded, column)

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


"""
col.py
"""


def test_select_col():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"))

    api_tester(fn)


def test_select_cols():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"), library.col("i"))

    api_tester(fn)


def test_select_col_arr():
    def fn(library: pk, frame: ft):
        return frame.select([library.col("u"), library.col("i")])

    api_tester(fn)


def test_select_name():
    def fn(_library: pk, frame: ft):
        return frame.select("u")

    api_tester(fn)


def test_select_names():
    def fn(_library: pk, frame: ft):
        return frame.select("u", "i")

    api_tester(fn)


def test_select_name_arr():
    def fn(_library: pk, frame: ft):
        return frame.select(["u", "i"])

    api_tester(fn)


"""
lit.py
"""


def test_lit():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"), library.lit("a").alias("A"))

    api_tester(fn)


"""
expr.py
"""


def test_expr_abs():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").abs())

    api_tester(fn)


def test_expr_add_num():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").add(10))

    api_tester(fn)


def test_expr_add_expr():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").add(library.col("u")))

    api_tester(fn)


def test_expr_alias():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").alias("U"))

    api_tester(fn)


def test_expr_and_int():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").and_(1))

    api_tester(fn)


def test_expr_and_bool():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").and_(True))

    api_tester(fn)


def test_expr_and_expr():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").and_(library.col("b")))

    api_tester(fn)


def test_expr_arccos():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arccos())

    api_tester(fn)


def test_expr_arccosh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arccosh())

    api_tester(fn)


def test_expr_arcsin():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arcsin())

    api_tester(fn)


def test_expr_arcsinh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arcsinh())

    api_tester(fn)


def test_expr_arctan():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arctan())

    api_tester(fn)


def test_expr_arctanh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").arctanh())

    api_tester(fn)


def test_expr_cast():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").cast(pl.Float64))

    api_tester(fn)


def test_expr_cbrt():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").cbrt())

    api_tester(fn)


def test_expr_ceil():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").ceil())

    api_tester(fn)


def test_expr_clip():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").clip(-1, 1))

    api_tester(fn)


def test_expr_cos():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").cos())

    api_tester(fn)


def test_expr_cosh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").cosh())

    api_tester(fn)


def test_expr_cot():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").cot())

    api_tester(fn)


def test_expr_degrees():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").degrees())

    api_tester(fn)


def test_expr_eq():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").eq(library.col("ff")))

    api_tester(fn)


def test_expr_eq_missing():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").eq_missing(library.col("ff")))

    api_tester(fn)


def test_expr_exp():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").exp())

    api_tester(fn)


def test_expr_fill_nan():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").fill_nan(10.0))

    api_tester(fn)


def test_expr_fill_null():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").fill_null(10.0))

    api_tester(fn)


def test_expr_filter():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(
            library.col("f").filter(library.col("i") > 0).sum().alias("sum")
        )

    api_tester(fn)


def test_expr_first():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"), library.col("f").first())

    api_tester(fn)


def test_expr_first_mmh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"), library.col("f").first())

    api_tester(fn)


def test_expr_floor():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").floor())

    api_tester(fn)


def test_expr_floordiv():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").floordiv(2))

    api_tester(fn)


def test_expr_ge():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").ge(library.col("ff")))

    api_tester(fn)


def test_expr_gt():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").gt(library.col("ff")))

    api_tester(fn)


def test_expr_hash():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").hash())

    api_tester(fn)


def test_expr_is_between():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").is_between(-1, 1))

    api_tester(fn)


def test_expr_is_finite():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_finite())

    api_tester(fn)


def test_expr_is_in():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").is_in([1, 2, 3]))

    api_tester(fn)


def test_expr_is_infinite():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_infinite())

    api_tester(fn)


def test_expr_is_nan():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_nan())

    api_tester(fn)


def test_expr_is_not_nan():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_not_nan())

    api_tester(fn)


def test_expr_is_not_null():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_not_null())

    api_tester(fn)


def test_expr_is_null():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_null())

    api_tester(fn)


def test_expr_is_unique():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").is_unique())

    api_tester(fn)


def test_expr_last():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u"), library.col("f").last())

    api_tester(fn)


@pytest.mark.skip("Pending decision on 1x1 polars frames.")
def test_expr_last_mmh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").last())

    api_tester(fn)


def test_expr_le():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").le(library.col("ff")))

    api_tester(fn)


def test_expr_log():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").log(2))

    api_tester(fn)


def test_expr_log1p():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").log1p())

    api_tester(fn)


def test_expr_log10():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").log10())

    api_tester(fn)


def test_expr_lt():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").lt(library.col("ff")))

    api_tester(fn)


def test_expr_mod():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").mod(library.col("u")))

    api_tester(fn)


def test_expr_mul():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").mul(library.col("u")))

    api_tester(fn)


def test_expr_ne():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").ne(library.col("ff")))

    api_tester(fn)


def test_expr_ne_missing():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").ne_missing(library.col("ff")))

    api_tester(fn)


def test_expr_neg():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").neg())

    api_tester(fn)


def test_expr_not_():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").not_())

    api_tester(fn)


def test_expr_or_int():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").or_(1))

    api_tester(fn)


def test_expr_or_bool():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").or_(True))

    api_tester(fn)


def test_expr_or_expr():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").or_(library.col("b")))

    api_tester(fn)


def test_expr_pow():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").pow(library.col("u")))

    api_tester(fn)


def test_expr_radians():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").radians())

    api_tester(fn)


def test_expr_rank():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").rank("max"))

    api_tester(fn)


def test_expr_diff():
    for name in ["i", "u", "f", "d", "dt"]:

        def fn(library: pk, frame: ft):
            return frame.select(library.col(name).diff())

        api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_expr_reinterpret():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").reinterpret(signed=False))

    api_tester(fn)


def test_expr_round():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").round())

    api_tester(fn)


def test_expr_round_round_sig_figs():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").round_sig_figs(2))

    api_tester(fn)


def test_expr_shrink_dtype():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").shrink_dtype())

    api_tester(fn)


def test_expr_sign():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").sign())

    api_tester(fn)


def test_expr_sin():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").sin())

    api_tester(fn)


def test_expr_sinh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").sinh())

    api_tester(fn)


def test_expr_count():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").count())

    api_tester(fn)


def test_expr_len():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").len())

    api_tester(fn)


def test_expr_min():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").min())

    api_tester(fn)


def test_expr_max():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").max())

    api_tester(fn)


# This test requires further investigation. Although essentially correct, false
# positives are observed on macOS x86_64.
# This is most probably related to numbers precision handling.
@pytest.mark.chipset
def test_expr_mean():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").mean())

    api_tester(fn, rounded=True, column="f")


def test_expr_median():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").median())

    api_tester(fn)


def test_expr_n_unique():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").n_unique())

    api_tester(fn)


def test_expr_slice():
    def fn(_library: pk, frame: ft):
        return frame.slice(1, 2)

    api_tester(fn)


def test_expr_sqrt():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").sqrt())

    api_tester(fn)


def test_expr_sub_num():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").sub(10))

    api_tester(fn)


def test_expr_sub_expr():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").sub(library.col("u")))

    api_tester(fn)


# This test requires further investigation. Although essentially correct, false
# positives are observed on macOS x86_64.
# This is most probably related to numbers precision handling.
@pytest.mark.chipset
def test_expr_sum():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("f").sum())

    api_tester(fn, rounded=True, column="f")


def test_expr_tan():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").tan())

    api_tester(fn)


def test_expr_tanh():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").tanh())

    api_tester(fn)


def test_expr_truediv():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("f").truediv(3))

    api_tester(fn)


def test_expr_xor_int():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("u").xor(1))

    api_tester(fn)


def test_expr_xor_bool():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").xor(True))

    api_tester(fn)


def test_expr_xor_expr():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("b").xor(library.col("b")))

    api_tester(fn)


def test_expr_dt():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("i").cast(pl.Datetime).dt.year())

    api_tester(fn)


def test_expr_str():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.to_uppercase())

    api_tester(fn)


def test_group_by_len():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").len()

    api_tester(fn, check=False)


def test_group_by_count():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").count()

    api_tester(fn, check=False)


def test_group_by_max():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").max()

    api_tester(fn)


def test_group_by_mean():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").mean()

    api_tester(fn)


def test_group_by_median():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").median()

    api_tester(fn)


def test_group_by_min():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").min()

    api_tester(fn)


def test_group_by_n_unique():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").n_unique()

    api_tester(fn)


def test_group_by_sum():
    def fn(_library: pk, frame: ft):
        return frame.group_by("ss").sum()

    api_tester(fn)

    def fn(library: pk, frame: ft):
        return frame.sort(library.col("f"))

    api_tester(fn, sort=False)


def test_frame_cast():
    def fn(_library: pk, frame: ft):
        return frame.cast({"i": pl.Float64})

    api_tester(fn)


def test_frame_clear():
    def fn(_library: pk, frame: ft):
        return frame.clear()

    api_tester(fn)


def test_frame_with_columns():
    def fn(library: pk, frame: ft):
        return frame.with_columns(library.col("i").alias("I"))

    api_tester(fn)


def test_frame_rename():
    def fn(library: pk, frame: ft):
        return frame.rename({"u": "uu", "i": "ii"})

    api_tester(fn)


def test_frame_rename_swap():
    def fn(library: pk, frame: ft):
        return frame.rename({"u": "i", "i": "u"})

    api_tester(fn)


def test_frame_drop():
    def fn(library: pk, frame: ft):
        return frame.drop(library.col("i"))

    api_tester(fn)


def test_frame_unnest():
    def fn(library: pk, frame: ft):
        return frame.unnest(library.col("info"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_STRUCT)


def test_frame_fill_null():
    def fn(_library: pk, frame: ft):
        return frame.fill_null(10)

    api_tester(fn)


def test_frame_fill_nan():
    def fn(_library: pk, frame: ft):
        return frame.fill_nan(10)

    api_tester(fn)


def test_frame_unique():
    def fn(library: pk, frame: ft):
        return frame.unique(library.col("ss"), keep="first")

    api_tester(fn)


def test_frame_drop_nans():
    def fn(library: pk, frame: ft):
        return frame.drop_nans(library.col("ff"))

    api_tester(fn)


def test_frame_drop_nulls():
    def fn(library: pk, frame: ft):
        return frame.drop_nulls(library.col("ff"))

    api_tester(fn)


def test_frame_filter():
    def fn(library: pk, frame: ft):
        return frame.filter(library.col("ff") > 0)

    api_tester(fn)


def test_frame_select():
    def fn(_library: pk, frame: ft):
        return frame.select(["u", "i"])

    api_tester(fn)


def test_frame_group_by():
    def fn(library: pk, frame: ft):
        return frame.group_by("ss").agg(library.col("u").sum())

    api_tester(fn)


def test_frame_slice():
    def fn(_library: pk, frame: ft):
        return frame.slice(1, 2)

    api_tester(fn)


def test_frame_limit():
    def fn(_library: pk, frame: ft):
        return frame.limit(2)

    api_tester(fn)


def test_frame_head():
    def fn(_library: pk, frame: ft):
        return frame.head(2)

    api_tester(fn)


def test_frame_tail():
    def fn(_library: pk, frame: ft):
        return frame.tail(2)

    api_tester(fn)


def test_frame_last():
    def fn(_library: pk, frame: ft):
        return frame.last()

    api_tester(fn)


def test_frame_first():
    def fn(_library: pk, frame: ft):
        return frame.first()

    api_tester(fn)


# TODO Tucu string functions
def test_str_to_date():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("d").str.to_date())

    api_tester(fn)


def test_str_to_datetime():
    def fn(library: pk, frame: ft):
        return frame.select(
            library.col("dt").str.to_datetime(
                format="%Y-%m-%d %H:%M:%SZ", time_zone="UTC"
            )
        )

    api_tester(fn)


def test_str_to_time():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("t").str.to_time())

    api_tester(fn)


def test_str_len_bytes():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("si").str.len_bytes())

    api_tester(fn)


def test_str_len_chars():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("si").str.len_chars())

    api_tester(fn)


def test_str_to_uppercase():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.to_uppercase())

    api_tester(fn)


def test_str_to_lowercase():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.to_lowercase())

    api_tester(fn)


def test_str_to_titlecase():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.to_titlecase())

    api_tester(fn)


def test_str_strip_chars():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.strip_chars("A"))

    api_tester(fn)


def test_str_strip_chars_start():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.strip_chars_start("A"))

    api_tester(fn)


def test_str_strip_chars_end():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.strip_chars_end("A"))

    api_tester(fn)


def test_str_strip_prefix():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.strip_prefix("A"))

    api_tester(fn)


def test_str_strip_suffix():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.strip_suffix("A"))

    api_tester(fn)


def test_str_pad_start():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.pad_start(5, "-"))

    api_tester(fn)


def test_str_pad_end():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.pad_end(5, "-"))

    api_tester(fn)


def test_str_zfill():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("si").str.zfill(2))

    api_tester(fn)


def test_str_contains():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.contains("A"))

    api_tester(fn)


def test_str_find():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.find("A"))

    api_tester(fn)


def test_str_ends_with():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.ends_with("A"))

    api_tester(fn)


def test_str_starts_with():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.starts_with("A"))

    api_tester(fn)


def test_str_extract():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.extract("(A)"))

    api_tester(fn)


def test_str_count_matches():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.count_matches("A"))

    api_tester(fn)


def test_str_replace():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.replace("A", "X"))

    api_tester(fn)


def test_str_replace_all():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.replace_all("A", "X"))

    api_tester(fn)


def test_str_reverse():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.reverse())

    api_tester(fn)


def test_str_slice():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.slice(1, 1))

    api_tester(fn)


def test_str_head():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.head(1))

    api_tester(fn)


def test_str_tail():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.tail(1))

    api_tester(fn)


def test_str_to_integer():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("si").str.to_integer())

    api_tester(fn)


def test_str_contains_any():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.contains_any(["A"]))

    api_tester(fn)


def test_str_replace_many():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("s").str.replace_many(["A"], "X"))

    api_tester(fn)


def test_datetime_add_business_day():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("d").dt.add_business_days(1, roll="forward"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_truncate():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.truncate("1h"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_replace():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.replace(year=2022))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_combine():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.combine(pl.time(1, 2, 3)))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_to_string():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.to_string())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_strftime():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.strftime("%Y-%Y-%Y"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_millennium():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.millennium())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_century():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.century())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_year():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.year())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_is_leap_year():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.is_leap_year())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_iso_year():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.iso_year())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_quarter():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.quarter())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_month():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.month())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_week():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.week())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_weekday():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.weekday())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_ordinal_day():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.ordinal_day())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_time():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.time())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_date():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.date())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_datetime():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.datetime())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_hour():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.hour())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_minute():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.minute())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_second():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.second())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_millisecond():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.millisecond())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_microsecond():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.microsecond())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_nanosecond():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.epoch())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_timestamp():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.timestamp())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_with_time_unit():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.with_time_unit("ms"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_cast_time_unit():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.cast_time_unit("ms"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_convert_time_zone():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.convert_time_zone("Europe/Paris"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_replace_time_zone():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.replace_time_zone("Europe/Paris"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_days():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_days())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_hours():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_hours())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_minutes():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_minutes())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_seconds():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_seconds())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_milliseconds():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_milliseconds())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_microseconds():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_microseconds())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_total_nanoseconds():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").diff().dt.total_nanoseconds())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_offset_by():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.offset_by("1mo"))

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_month_start():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.month_start())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_month_end():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.month_end())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_base_utc_offset():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.base_utc_offset())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)


def test_datetime_dst_offset():
    def fn(library: pk, frame: ft):
        return frame.select(library.col("dt").dt.dst_offset())

    api_tester(fn, polars_frame=POLARS_TABLE_FRAME_DATETIME)
