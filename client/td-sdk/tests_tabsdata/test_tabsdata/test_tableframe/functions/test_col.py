#
# Copyright 2025 Tabs Data Inc.
#

import logging

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._helpers import system_columns
from tabsdata.exceptions import ErrorCode, TabsDataException
from tabsdata.tableframe.expr.expr import Expr

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_regular_expression():
    with pytest.raises(TabsDataException) as error:
        td.col("^ko*$")
        assert error.value.error_code == ErrorCode.TF3


def test_reserved_column():
    for name in system_columns():
        with pytest.raises(TabsDataException) as error:
            td.col(name)
            assert error.value.error_code == ErrorCode.TF4


def test_common_column():
    assert isinstance(td.col("my_column"), Expr)
    assert isinstance(td.col("^my_column"), Expr)
    assert isinstance(td.col("$my_column"), Expr)
    assert isinstance(td.col("my_column^"), Expr)
    assert isinstance(td.col("my_column$"), Expr)
    assert isinstance(td.col("$my_column^"), Expr)
    assert isinstance(td.col("*my_column"), Expr)
    assert isinstance(td.col("my*column"), Expr)
    assert isinstance(td.col("*my_column*"), Expr)


def test_polars_examples():
    _ = td.col("foo") * td.col("bar")
    _ = td.col.foo + td.col.bar
    _ = td.col("foo")
    _ = td.col("*")
    _ = td.col("*")
    _ = td.col(["hamburger", "foo"])
    _ = td.col("hamburger", "foo")
    _ = td.col(pl.String)
    _ = td.col(pl.Int64, pl.Float64)
