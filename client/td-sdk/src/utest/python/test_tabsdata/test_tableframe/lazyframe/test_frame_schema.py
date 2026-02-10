#
# Copyright 2025 Tabs Data Inc.
#

import logging

import polars as pl
import pytest

import tabsdata as td
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture
def base_tableframe():
    data = {"a": ["A", "B"], "b": [1, 2], "c": [3, 4]}
    return td.TableFrame.__build__(
        df=pl.LazyFrame(data),
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )


def test_has_cols_invalid_type_int(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols(123)


def test_has_cols_invalid_type_dict(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols({"a": 1})


def test_has_cols_invalid_type_tuple(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols(("a", "b"))


def test_has_cols_all_present(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "b"]
    )
    assert success is True
    assert missing_in_tableframe == set()
    assert missing_in_expected == {"c"}


def test_has_cols_some_missing(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "d"]
    )
    assert success is False
    assert missing_in_tableframe == {"d"}
    assert missing_in_expected == {"b", "c"}


def test_has_cols_single_column(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols("a")
    assert success is True
    assert missing_in_tableframe == set()
    assert missing_in_expected == {"b", "c"}


def test_has_cols_all_missing(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["x", "y"]
    )
    assert success is False
    assert missing_in_tableframe == {"x", "y"}
    assert missing_in_expected == {"a", "b", "c"}


def test_has_cols_exact_invalid_type_int(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols(123, exact=True)


def test_has_cols_exact_invalid_type_dict(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols({"a": 1}, exact=True)


def test_has_cols_exact_invalid_type_tuple(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.has_cols(("a", "b"), exact=True)


def test_has_cols_exact_perfect_match(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "b", "c"], exact=True
    )
    assert success is True
    assert missing_in_tableframe == set()
    assert missing_in_expected == set()


def test_has_cols_exact_extra_in_tableframe(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "b"], exact=True
    )
    assert success is False
    assert missing_in_tableframe == set()
    assert missing_in_expected == {"c"}


def test_has_cols_exact_extra_in_expected(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "b", "c", "d"], exact=True
    )
    assert success is False
    assert missing_in_tableframe == {"d"}
    assert missing_in_expected == set()


def test_has_cols_exact_missing_in_both(base_tableframe):
    success, missing_in_tableframe, missing_in_expected = base_tableframe.has_cols(
        ["a", "d"], exact=True
    )
    assert success is False
    assert missing_in_tableframe == {"d"}
    assert missing_in_expected == {"b", "c"}


def test_assert_has_cols_invalid_type_int(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols(123)


def test_assert_has_cols_invalid_type_dict(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols({"a": 1})


def test_assert_has_cols_invalid_type_tuple(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols(("a", "b"))


def test_assert_has_cols_all_present(base_tableframe):
    base_tableframe.assert_has_cols(["a", "b"])


def test_assert_has_cols_some_missing(base_tableframe):
    with pytest.raises(ValueError):
        base_tableframe.assert_has_cols(["a", "d"])


def test_assert_has_cols_single_column(base_tableframe):
    base_tableframe.assert_has_cols("a")


def test_assert_has_cols_all_missing(base_tableframe):
    with pytest.raises(ValueError):
        base_tableframe.assert_has_cols(["x", "y"])


def test_assert_has_cols_exact_invalid_type_int(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols(123, exact=True)


def test_assert_has_cols_exact_invalid_type_dict(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols({"a": 1}, exact=True)


def test_assert_has_cols_exact_invalid_type_tuple(base_tableframe):
    with pytest.raises(TypeError):
        base_tableframe.assert_has_cols(("a", "b"), exact=True)


def test_assert_has_cols_exact_perfect_match(base_tableframe):
    base_tableframe.assert_has_cols(["a", "b", "c"], exact=True)


def test_assert_has_cols_exact_extra_in_tableframe(base_tableframe):
    with pytest.raises(ValueError):
        base_tableframe.assert_has_cols(["a", "b"], exact=True)


def test_assert_has_cols_exact_extra_in_expected(base_tableframe):
    with pytest.raises(ValueError):
        base_tableframe.assert_has_cols(["a", "b", "c", "d"], exact=True)


def test_assert_has_cols_exact_missing_in_both(base_tableframe):
    with pytest.raises(ValueError):
        base_tableframe.assert_has_cols(["a", "d"], exact=True)


@pytest.fixture
def tf1():
    return td.TableFrame.from_polars(pl.DataFrame({"a": ["A"], "b": [1]}))


@pytest.fixture
def tf2():
    return td.TableFrame.from_polars(pl.DataFrame({"a": ["B"], "b": [2]}))


@pytest.fixture
def tf_diff_cols():
    return td.TableFrame.from_polars(pl.DataFrame({"a": ["A"], "c": [1]}))


@pytest.fixture
def tf_diff_types():
    return td.TableFrame.from_polars(pl.DataFrame({"a": ["A"], "b": ["1"]}))


def test_has_same_schema_true(tf1, tf2):
    assert tf1.has_same_schema(tf2) is True


def test_has_same_schema_diff_columns(tf1, tf_diff_cols):
    assert tf1.has_same_schema(tf_diff_cols) is False


def test_has_same_schema_diff_types(tf1, tf_diff_types):
    assert tf1.has_same_schema(tf_diff_types) is False


def test_has_same_schema_self(tf1):
    assert tf1.has_same_schema(tf1) is True
