#
# Copyright 2025 Tabs Data Inc.
#

import logging
import unittest

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._helpers import SYSTEM_COLUMNS

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import _wrap_polars_frame
from tabsdata.exceptions import ErrorCode, TabsDataException

# noinspection PyProtectedMember
from tabsdata.tableframe.expr.expr import Expr

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe, pretty_polars

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

""" Dunder Operations """


def test___repr__():
    expr = td.col("a")
    assert repr(expr) == repr(expr._expr)


def test__str__():
    expr = td.col("a")
    assert str(expr) == str(expr._expr)


def test__bool__():
    expr = td.col("a")
    with pytest.raises(TypeError):
        bool(expr)


def test__abs__():
    expr = td.col("a")
    assert str(abs(expr)) == str(abs(expr._expr))


def test__add__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    expr + td.col("b")


def test__radd__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    td.col("b") + expr


def test__and__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    expr & td.col("b")


def test__rand__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    td.col("b") & expr


def test__eq__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr == td.col("b")


def test__floordiv__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    expr // td.col("b")


def test__rfloordiv__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    td.col("b") // expr


def test__ge__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr >= td.col("b")


def test__gt__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr > td.col("b")


def test__invert__():
    expr = td.col("a")
    assert str(~expr) == str(Expr(~expr._expr))


def test__le__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr <= td.col("b")


def test__lt__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr < td.col("b")


def test__mod__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr % td.col("b")


def test__rmod__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") % expr


def test__mul__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr * td.col("b")


def test__rmul__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") * expr


def test__ne__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr != td.col("b")


def test__neg__():
    expr = td.col("a")
    assert str(-expr) == str(Expr(-expr._expr))


def test__or__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr | td.col("b")


def test__ror__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") | expr


def test__pos__():
    expr = td.col("a")
    assert str(+expr) == str(Expr(expr._expr + expr._expr))


def test__pow__():
    expr = td.col("a")
    _ = expr**2


def test__rpow__():
    expr = td.col("a")
    _ = 2**expr


def test__sub__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr - td.col("b")


def test__rsub__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") - expr


def test__truediv__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr / td.col("b")


def test__rtruediv__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") / expr


def test__xor__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = expr ^ td.col("b")


def test__rxor__():
    expr = td.col("a")
    # noinspection PyTypeChecker
    _ = td.col("b") ^ expr


def test__getstate__():
    expr = td.col("a")
    assert expr.__getstate__() == expr._expr.__getstate__()


def test__setstate__():
    expr = td.col("a")
    state = expr.__getstate__()
    expr.__setstate__(state)
    assert expr._expr.__getstate__() == state


""" Object Operations """


def test_alias():
    for name in SYSTEM_COLUMNS:
        with pytest.raises(TabsDataException) as error:
            td.col("dame").alias(name)
            assert error.value.error_code == ErrorCode.TF4


def test_filter():
    td.col("a").filter("0 == 0")


def test_is_between():
    td.col("*").is_between(0, 0)


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        pretty_polars()

        self.data_frame, self.lazy_frame, self.table_frame = load_complex_dataframe(
            token="l"
        )

    def test_filter(self):
        lf = pl.LazyFrame(
            {
                "letters": ["a", "b", "c"],
                "numbers": [1, 2, 3],
            }
        )
        tf = _wrap_polars_frame(lf)
        tf = tf.filter(
            (td.col("numbers") > 1).and_(td.col("letters").str.starts_with("b"))
        )
        tf._lf.collect()

    def test_agg(self):
        data = [
            {"letters": "a", "numbers": 1, "dates": "2024-02-18"},
            {"letters": "b", "numbers": 2, "dates": "2024-09-09"},
            {"letters": "c", "numbers": 3, "dates": "2024-03-28"},
            {"letters": "d", "numbers": 4, "dates": "2024-07-14"},
            {"letters": "e", "numbers": 5, "dates": "2024-04-06"},
            {"letters": "f", "numbers": 6, "dates": "2024-07-22"},
            {"letters": "g", "numbers": 7, "dates": "2023-08-26"},
            {"letters": "h", "numbers": 8, "dates": "2023-07-27"},
        ]
        lf = pl.from_dicts(data).lazy()
        tf = _wrap_polars_frame(lf)
        tf = tf.group_by(td.col("dates").cast(pl.Date).dt.year().alias("year")).agg(
            td.col("letters").count()
        )
        tf._lf.collect()
