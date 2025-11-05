#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from pycountries import Country

import tabsdata as td
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()


USA_CODES = frozenset([Country.US.alpha_2, Country.US.alpha_3])
US_ZIP_PATTERN = r"^\d{5}(-\d{4})?$"


def is_valid_zip(country_column_name: str, zip_column_name: str) -> pl.Expr:
    is_usa = (
        pl.col(country_column_name)
        .str.strip_chars()
        .str.to_uppercase()
        .is_in(USA_CODES)
    )
    zip_ok = (
        pl.col(zip_column_name)
        .str.strip_chars()
        .str.contains(US_ZIP_PATTERN)
        .fill_null(False)
    )
    return ~is_usa | zip_ok


@pytest.mark.dq
def test_valid_zip():
    lf = pl.LazyFrame(
        {
            "country": ["US", "USA", "CA", "ES", "US", None],
            "zip": ["a-b-c", "94107-1234", "H3Z 2Y7", "08001", None, "12345"],
        }
    )
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    expected = [False, True, True, True, False, True]

    dq = tf.dq.expr(is_valid_zip("country", "zip"), "valid_zip")
    df = dq.tf()._lf.collect()

    assert "valid_zip" in df.columns
    assert df["valid_zip"].dtype == pl.Boolean
    assert df["valid_zip"].to_list() == expected
