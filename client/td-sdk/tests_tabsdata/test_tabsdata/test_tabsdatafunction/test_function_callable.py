#
# Copyright 2025 Tabs Data Inc.
#

from typing import List

import pandas as pd
import polars as pl

import tabsdata as td
from tabsdata import TableFrame
from tabsdata.tabsdatafunction import (
    _clean_recursively_and_convert_to_datatype,
    _convert_recursively_to_tableframe,
    _recursively_obtain_datatype,
)
from tabsdata.utils.tableframe._common import add_system_columns
from tabsdata.utils.tableframe._helpers import SYSTEM_COLUMNS

BASE_DATA = {"a": [1, 2, 3], "b": [4, 5, 6]}
VALID_PD_DATAFRAME = pd.DataFrame(BASE_DATA)
VALID_PL_DATAFRAME = pl.DataFrame(BASE_DATA)
VALID_PL_LAZYFRAME = pl.LazyFrame(BASE_DATA)
# Only for testing; thus, using index None does the trick.
VALID_TD_TABLEFRAME = TableFrame.__build__(
    df=pl.DataFrame(BASE_DATA),
    mode="raw",
    idx=None,
)


def test_has_required_columns_polars_dataframe():
    df = pl.DataFrame({"a": [1, 2, 3, 4], "b": [4, 5, 6, 7]})
    df = add_system_columns(lf=df.lazy(), mode="raw", idx=None).collect()
    assert all(col in df.columns for col in SYSTEM_COLUMNS)


def test_has_required_columns_empty_polars_dataframe():
    df = pl.DataFrame({})
    df = add_system_columns(lf=df.lazy(), mode="raw", idx=None).collect()
    assert all(col in df.columns for col in SYSTEM_COLUMNS)


def test_has_required_columns_polars_lazyframe():
    df = pl.LazyFrame({"a": [1, 2, 3, 4], "b": [4, 5, 6, 7]})
    df = add_system_columns(lf=df, mode="raw", idx=None)
    assert all(col in df.collect_schema().names() for col in SYSTEM_COLUMNS)


def test_has_required_columns_empty_polars_lazyframe():
    df = pl.LazyFrame({})
    df = add_system_columns(lf=df, mode="raw", idx=None)
    assert all(col in df.collect_schema().names() for col in SYSTEM_COLUMNS)


def test_recursively_obtain_datatype_single_object():
    arg = VALID_PL_DATAFRAME
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = VALID_PL_LAZYFRAME
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = VALID_TD_TABLEFRAME
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = VALID_PD_DATAFRAME
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_tuple():
    arg = (VALID_PL_DATAFRAME,)
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = (VALID_PL_LAZYFRAME,)
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = (VALID_TD_TABLEFRAME,)
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = (VALID_PD_DATAFRAME,)
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_list():
    arg = [VALID_PL_DATAFRAME]
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = [VALID_PL_LAZYFRAME]
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = [VALID_TD_TABLEFRAME]
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = [VALID_PD_DATAFRAME]
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_list_of_list():
    arg = [[VALID_PL_DATAFRAME]]
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = [[VALID_PL_LAZYFRAME]]
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = [[VALID_TD_TABLEFRAME]]
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = [[VALID_PD_DATAFRAME]]
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_dict():
    arg = {"a": VALID_PL_DATAFRAME}
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = {"a": VALID_PL_LAZYFRAME}
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = {"a": VALID_TD_TABLEFRAME}
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = {"a": VALID_PD_DATAFRAME}
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_dict_of_list():
    arg = {"a": [VALID_PL_DATAFRAME]}
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = {"a": [VALID_PL_LAZYFRAME]}
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = {"a": [VALID_TD_TABLEFRAME]}
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = {"a": [VALID_PD_DATAFRAME]}
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_tuple_of_list():
    arg = ([VALID_PL_DATAFRAME],)
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = ([VALID_PL_LAZYFRAME],)
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = ([VALID_TD_TABLEFRAME],)
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = ([VALID_PD_DATAFRAME],)
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_tuple_of_list_of_list():
    arg = ([[VALID_PL_DATAFRAME]],)
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = ([[VALID_PL_LAZYFRAME]],)
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = ([[VALID_TD_TABLEFRAME]],)
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = ([[VALID_PD_DATAFRAME]],)
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_tuple_of_dict():
    arg = ({"a": VALID_PL_DATAFRAME},)
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = ({"a": VALID_PL_LAZYFRAME},)
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = ({"a": VALID_TD_TABLEFRAME},)
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = ({"a": VALID_PD_DATAFRAME},)
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_tuple_of_dict_of_list():
    arg = ({"a": [VALID_PL_DATAFRAME]},)
    assert _recursively_obtain_datatype(arg) == pl.DataFrame
    arg = ({"a": [VALID_PL_LAZYFRAME]},)
    assert _recursively_obtain_datatype(arg) == pl.LazyFrame
    arg = ({"a": [VALID_TD_TABLEFRAME]},)
    assert _recursively_obtain_datatype(arg) == TableFrame
    arg = ({"a": [VALID_PD_DATAFRAME]},)
    assert _recursively_obtain_datatype(arg) == pd.DataFrame


def test_recursively_obtain_datatype_deep_list():
    arg = [[[[[], []], [], [VALID_PL_DATAFRAME]], []]]
    assert _recursively_obtain_datatype(arg) == pl.DataFrame


def test_recursively_obtain_datatype_deep_dictionary():
    arg = {
        "a": {"b": []},
        "c": {},
        "d": ({"e": VALID_PL_DATAFRAME},),
    }
    assert _recursively_obtain_datatype(arg) == pl.DataFrame


def test_recursively_obtain_datatype_empty_objects():
    arg = []
    assert _recursively_obtain_datatype(arg) is None
    arg = {}
    assert _recursively_obtain_datatype(arg) is None
    arg = ()
    assert _recursively_obtain_datatype(arg) is None
    arg = None
    assert _recursively_obtain_datatype(arg) is None


def test_convert_recursively_to_tableframe_single_object():
    arg = VALID_PL_DATAFRAME
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup, pl.DataFrame)
    assert cleanup.equals(arg)

    arg = VALID_PL_LAZYFRAME
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup, pl.LazyFrame)
    assert cleanup.collect().equals(arg.collect())

    arg = VALID_PD_DATAFRAME
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup, pd.DataFrame)
    assert cleanup.equals(arg)


def test_convert_recursively_to_tableframe_tuple():
    arg = (VALID_PL_DATAFRAME,)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0], pl.DataFrame)
    assert cleanup[0].equals(VALID_PL_DATAFRAME)

    arg = (VALID_PL_LAZYFRAME,)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0], pl.LazyFrame)
    assert cleanup[0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = (VALID_PD_DATAFRAME,)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0], pd.DataFrame)
    assert cleanup[0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_list():
    arg = [VALID_PL_DATAFRAME]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0], pl.DataFrame)
    assert cleanup[0].equals(VALID_PL_DATAFRAME)

    arg = [VALID_PL_LAZYFRAME]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0], pl.LazyFrame)
    assert cleanup[0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = [VALID_PD_DATAFRAME]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0], pd.DataFrame)
    assert cleanup[0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_list_of_list():
    arg = [[VALID_PL_DATAFRAME]]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], list)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0][0], pl.DataFrame)
    assert cleanup[0][0].equals(VALID_PL_DATAFRAME)

    arg = [[VALID_PL_LAZYFRAME]]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], list)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0][0], pl.LazyFrame)
    assert cleanup[0][0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = [[VALID_PD_DATAFRAME]]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0], list)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0][0], pd.DataFrame)
    assert cleanup[0][0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_dict():
    arg = {"a": VALID_PL_DATAFRAME}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup["a"], pl.DataFrame)
    assert cleanup["a"].equals(VALID_PL_DATAFRAME)

    arg = {"a": VALID_PL_LAZYFRAME}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup["a"], pl.LazyFrame)
    assert cleanup["a"].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = {"a": VALID_PD_DATAFRAME}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup["a"], pd.DataFrame)
    assert cleanup["a"].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_dict_of_list():
    arg = {"a": [VALID_PL_DATAFRAME]}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup["a"][0], pl.DataFrame)
    assert cleanup["a"][0].equals(VALID_PL_DATAFRAME)

    arg = {"a": [VALID_PL_LAZYFRAME]}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup["a"][0], pl.LazyFrame)
    assert cleanup["a"][0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = {"a": [VALID_PD_DATAFRAME]}
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup["a"][0], pd.DataFrame)
    assert cleanup["a"][0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_tuple_of_list():
    arg = ([VALID_PL_DATAFRAME],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0][0], pl.DataFrame)
    assert cleanup[0][0].equals(VALID_PL_DATAFRAME)

    arg = ([VALID_PL_LAZYFRAME],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0][0], pl.LazyFrame)
    assert cleanup[0][0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = ([VALID_PD_DATAFRAME],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0][0], pd.DataFrame)
    assert cleanup[0][0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_tuple_of_list_of_list():
    arg = ([[VALID_PL_DATAFRAME]],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0][0][0], pl.DataFrame)
    assert cleanup[0][0][0].equals(VALID_PL_DATAFRAME)

    arg = ([[VALID_PL_LAZYFRAME]],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0][0][0], pl.LazyFrame)
    assert cleanup[0][0][0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = ([[VALID_PD_DATAFRAME]],)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0][0][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0][0][0], pd.DataFrame)
    assert cleanup[0][0][0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_tuple_of_dict():
    arg = ({"a": VALID_PL_DATAFRAME},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0]["a"], pl.DataFrame)
    assert cleanup[0]["a"].equals(VALID_PL_DATAFRAME)

    arg = ({"a": VALID_PL_LAZYFRAME},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0]["a"], pl.LazyFrame)
    assert cleanup[0]["a"].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = ({"a": VALID_PD_DATAFRAME},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0]["a"], pd.DataFrame)
    assert cleanup[0]["a"].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_tuple_of_dict_of_list():
    arg = ({"a": [VALID_PL_DATAFRAME]},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0]["a"][0], pl.DataFrame)
    assert cleanup[0]["a"][0].equals(VALID_PL_DATAFRAME)

    arg = ({"a": [VALID_PL_LAZYFRAME]},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.LazyFrame)
    assert isinstance(cleanup[0]["a"][0], pl.LazyFrame)
    assert cleanup[0]["a"][0].collect().equals(VALID_PL_LAZYFRAME.collect())

    arg = ({"a": [VALID_PD_DATAFRAME]},)
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, tuple)
    assert isinstance(result[0]["a"][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pd.DataFrame)
    assert isinstance(cleanup[0]["a"][0], pd.DataFrame)
    assert cleanup[0]["a"][0].equals(VALID_PD_DATAFRAME)


def test_convert_recursively_to_tableframe_deep_list():
    arg = [[[[[], []], [], [VALID_PL_DATAFRAME]], []]]
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, list)
    assert isinstance(result[0][0][2][0], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup[0][0][2][0], pl.DataFrame)
    assert cleanup[0][0][2][0].equals(VALID_PL_DATAFRAME)


def test_convert_recursively_to_tableframe_deep_dictionary():
    arg = {
        "a": {"b": []},
        "c": {},
        "d": ({"e": VALID_PL_DATAFRAME},),
    }
    result = _convert_recursively_to_tableframe(arg)
    assert isinstance(result, dict)
    assert isinstance(result["d"][0]["e"], TableFrame)
    cleanup = _clean_recursively_and_convert_to_datatype(result, pl.DataFrame)
    assert isinstance(cleanup["d"][0]["e"], pl.DataFrame)
    assert cleanup["d"][0]["e"].equals(VALID_PL_DATAFRAME)


def test_convert_recursively_to_tableframe_empty_objects():
    arg = []
    assert _convert_recursively_to_tableframe(arg) == []
    arg = {}
    assert _convert_recursively_to_tableframe(arg) == {}
    arg = ()
    assert _convert_recursively_to_tableframe(arg) == ()
    arg = None
    assert _convert_recursively_to_tableframe(arg) is None


def test_clean_recursively_and_convert_to_datatype_empty_objects():
    arg = []
    assert _clean_recursively_and_convert_to_datatype(arg, pl.DataFrame) == []
    arg = {}
    assert _clean_recursively_and_convert_to_datatype(arg, pl.DataFrame) == {}
    arg = ()
    assert _clean_recursively_and_convert_to_datatype(arg, pl.DataFrame) == ()
    arg = None
    assert _clean_recursively_and_convert_to_datatype(arg, pl.DataFrame) is None


@td.transformer(["a", "b"], ["c", "d"])
def dummy_transformer(a: td.TableFrame, b: td.TableFrame):
    if not isinstance(a, TableFrame):
        raise ValueError("a is not a TableFrame")
    if not isinstance(b, TableFrame):
        raise ValueError("b is not a TableFrame")
    return b, a


def test_call_transformer_positional():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pl.DataFrame)
    assert isinstance(d, pl.DataFrame)

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b)
    assert c.collect().equals(b.collect())
    assert d.collect().equals(a.collect())

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pd.DataFrame)
    assert isinstance(d, pd.DataFrame)


def test_call_transformer_keyword():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(b=b, a=a)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pl.DataFrame)
    assert isinstance(d, pl.DataFrame)

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(b=b, a=a)
    assert c.collect().equals(b.collect())
    assert d.collect().equals(a.collect())
    assert isinstance(c, pl.LazyFrame)
    assert isinstance(d, pl.LazyFrame)

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(b=b, a=a)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pd.DataFrame)
    assert isinstance(d, pd.DataFrame)


def test_call_transformer_hybrid():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b=b)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pl.DataFrame)
    assert isinstance(d, pl.DataFrame)

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b=b)
    assert c.collect().equals(b.collect())
    assert d.collect().equals(a.collect())
    assert isinstance(c, pl.LazyFrame)
    assert isinstance(d, pl.LazyFrame)

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    c, d = dummy_transformer(a, b=b)
    assert c.equals(b)
    assert d.equals(a)
    assert isinstance(c, pd.DataFrame)
    assert isinstance(d, pd.DataFrame)


@td.transformer(["a", "b"], ["c", "d"])
def dummy_list_transformer(a: List[pl.DataFrame], b: List[pl.DataFrame]):
    for element in a:
        if not isinstance(element, TableFrame):
            raise ValueError("a is not a list of TableFrame")
    for element in b:
        if not isinstance(element, TableFrame):
            raise ValueError("b is not a list of TableFrame")
    return b, a


def test_call_transformer_list_positional():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.LazyFrame) for i in range(3))
    assert all(c[i].collect().equals(b.collect()) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.LazyFrame) for i in range(3))
    assert all(d[i].collect().equals(a.collect()) for i in range(3))

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pd.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pd.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))


def test_call_transformer_list_keyword():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(b=second_list, a=first_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(b=second_list, a=first_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.LazyFrame) for i in range(3))
    assert all(c[i].collect().equals(b.collect()) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.LazyFrame) for i in range(3))
    assert all(d[i].collect().equals(a.collect()) for i in range(3))

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(b=second_list, a=first_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pd.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pd.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))


def test_call_transformer_list_hybrid():
    a = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, b=second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))

    a = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pl.LazyFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, b=second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pl.LazyFrame) for i in range(3))
    assert all(c[i].collect().equals(b.collect()) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pl.LazyFrame) for i in range(3))
    assert all(d[i].collect().equals(a.collect()) for i in range(3))

    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    b = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    first_list = [a, a, a]
    second_list = [b, b, b]
    c, d = dummy_list_transformer(first_list, b=second_list)
    assert len(c) == 3
    assert all(isinstance(c[i], pd.DataFrame) for i in range(3))
    assert all(c[i].equals(b) for i in range(3))
    assert len(d) == 3
    assert all(isinstance(d[i], pd.DataFrame) for i in range(3))
    assert all(d[i].equals(a) for i in range(3))
