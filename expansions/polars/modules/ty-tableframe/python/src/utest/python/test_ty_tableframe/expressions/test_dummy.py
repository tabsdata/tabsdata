#
# Copyright 2025 Tabs Data Inc.
#

import logging

import polars as pl

from tabsdata.expansions.tableframe.expressions import dummy_expr

logger = logging.getLogger(__name__)


def test_dummy_expr():
    lf = pl.LazyFrame({"input_column": ["1", "2", "3", "4", "5"] * 5})
    df = lf.collect()
    assert df.shape == (25, 1)
    lf = lf.with_columns(dummy_expr("input_column").alias("dummy_column"))
    df = lf.collect()
    assert df.shape == (25, 2)
    assert df.shape[0] == 25
    assert "dummy_column" in df.columns
    assert df.shape[1] >= 1
    assert df.schema["dummy_column"] == pl.String
    assert df.select(pl.col("dummy_column").n_unique()).item() == 1
    assert df["dummy_column"][0] == "dummy string"
    assert df["dummy_column"].to_list() == ["dummy string"] * 25
