#
# Copyright 2024 Tabs Data Inc.
#

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata.exceptions import ErrorCode, TabsDataException
from tabsdata.extensions.features.api.features import Feature, FeaturesManager
from tabsdata.extensions.tableframe.extension import TableFrameExtension
from tabsdata.tableframe.lazyframe.frame import TableFrame

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._helpers import REQUIRED_COLUMNS

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import _wrap_polars_frame

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401


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
