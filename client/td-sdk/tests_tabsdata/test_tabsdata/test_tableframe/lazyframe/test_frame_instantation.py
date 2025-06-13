#
# Copyright 2024 Tabs Data Inc.
#

from typing import Any

import pandas as pd
import polars as pl

# noinspection PyPackageRequirements
import pytest
from tabsdata.utils.tableframe.builders import from_dict, from_polars, from_pandas
from tabulate import tabulate

import tabsdata as td
from tabsdata.exceptions import ErrorCode, TabsDataException
from tabsdata.extensions.features.api.features import Feature, FeaturesManager
from tabsdata.extensions.tableframe.extension import TableFrameExtension
from tabsdata.tableframe.lazyframe.frame import TableFrame, TableFrameOrigin

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._helpers import REQUIRED_COLUMNS

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import _wrap_polars_frame

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe, pretty_pandas, pretty_polars


def test_feature():
    enterprise = FeaturesManager.instance().is_enabled(Feature.ENTERPRISE)
    summary = TableFrameExtension.instance().summary
    if enterprise:
        assert summary == "Enterprise"
    else:
        assert summary == "Standard"


def test_init_with_dataframe_with_required_columns():
    data = {col: [1, 2, 3] for col in REQUIRED_COLUMNS}
    df = pl.DataFrame(data)
    tdf = _wrap_polars_frame(df)
    assert isinstance(tdf, td.TableFrame)


def test_init_with_lazyframe_with_required_columns():
    data = {col: [1, 2, 3] for col in REQUIRED_COLUMNS}
    d = pl.DataFrame(data).to_dict(as_series=False)
    tdf = TableFrame.__build__(
        df=d,
        mode="raw",
        idx=None,
    )
    assert isinstance(tdf, td.TableFrame)


def test_init_with_tabsdata_lazyframe():
    data = {col: [1, 2, 3] for col in REQUIRED_COLUMNS}
    df = pl.DataFrame(data)
    tdf_i = _wrap_polars_frame(df)
    tdf_o = td.TableFrame.__build__(
        df=tdf_i,
        mode="raw",
        idx=None,
    )
    assert isinstance(tdf_o, td.TableFrame)


def test_init_with_string():
    data = "one_string"
    with pytest.raises(TabsDataException) as error:
        # noinspection PyTypeChecker
        td.TableFrame.__build__(
            df=data,
            mode="raw",
            idx=None,
        )
    assert error.value.error_code == ErrorCode.TF2


def test_builder():
    pretty_polars()
    pretty_pandas()

    print("")

    _, _, tf = load_complex_dataframe()
    print(f">>> tf\n:{tf._lf.collect()}")

    pandas: pd.DataFrame = tf.to_pandas()
    print(f">>> pandas:\n{pandas}")

    polars_df: pl.DataFrame = tf.to_polars_df()
    print(f">>> polars df\n:{polars_df}")

    polars_lf: pl.LAzyFrame = tf.to_polars_lf()
    print(f">>> polars lf\n:{polars_lf.collect()}")

    dictionary: dict[str, list[Any]] = tf.to_dict()
    print(f">>> dictionary\n:")
    headers = dictionary.keys()
    rows = list(zip(*dictionary.values()))
    print(tabulate(rows, headers=list(headers), tablefmt="github"))

    tf: td.TableFrame = from_pandas(pandas)
    print(f">>> pandas tf:\n{tf._lf.collect()}")

    tf: td.TableFrame = from_polars(polars_df)
    print(f">>> polars df tf:\n{tf._lf.collect()}")

    tf: td.TableFrame = from_polars(polars_lf)
    print(f">>> polars lf tf:\n{tf._lf.collect()}")

    tf: td.TableFrame = from_dict(dictionary)
    print(f">>> dictionary tf:\n{tf._lf.collect()}")


def assert_origin(tf: TableFrame, expected: TableFrameOrigin):
    # noinspection PyProtectedMember
    assert (
        tf._origin.value == expected.value
    ), f"Expected origin {expected}, got {tf._origin}"


def test_empty():
    tf = TableFrame.empty()
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_dict():
    tf = TableFrame.from_dict({"a": [1]})
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_dict_none():
    tf = TableFrame.from_dict(None)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_pandas():
    df = pd.DataFrame({"a": [1]})
    tf = TableFrame.from_pandas(df)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_pandas_none():
    tf = TableFrame.from_pandas(None)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_polars_df():
    df = pl.DataFrame({"a": [1]})
    tf = TableFrame.from_polars(df)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_polars_lf():
    lf = pl.LazyFrame({"a": [1]})
    tf = TableFrame.from_polars(lf)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_from_polars_none():
    tf = TableFrame.from_polars(None)
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_build():
    tf = TableFrame.__build__(df={"a": [1]}, mode="raw", idx=None)
    assert_origin(tf, TableFrameOrigin.BUILD)


def test_build_explicit():
    tf = TableFrame.__build__(
        origin=TableFrameOrigin.IMPORT, df={"a": [1]}, mode="raw", idx=None
    )
    assert_origin(tf, TableFrameOrigin.IMPORT)


def test_init_from_tableframe():
    tf1 = TableFrame.from_dict({"a": [1]})
    tf2 = TableFrame(tf1)
    assert_origin(tf2, TableFrameOrigin.IMPORT)


def test_init_from_tableframe_explicit():
    tf1 = TableFrame.from_dict({"a": [1]})
    tf2 = TableFrame(tf1, origin=TableFrameOrigin.IMPORT)
    assert_origin(tf2, TableFrameOrigin.IMPORT)


def test_init_from_dict():
    tf = TableFrame({"a": [1]}, origin=None)
    assert_origin(tf, TableFrameOrigin.INIT)


def test_init_from_none():
    tf = TableFrame(None, origin=None)
    assert_origin(tf, TableFrameOrigin.INIT)


def test_init_from_void():
    tf = TableFrame(origin=None)
    assert_origin(tf, TableFrameOrigin.INIT)


def test_init_from_dict_explicit():
    tf = TableFrame({"a": [1]}, origin=TableFrameOrigin.IMPORT)
    assert_origin(tf, TableFrameOrigin.IMPORT)
