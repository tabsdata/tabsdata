#
# Copyright 2025 Tabs Data Inc.
#
import logging

import polars as pl

from tabsdata.expansions.tableframe import dummy

logger = logging.getLogger(__name__)


def test_dummy():
    assert 1 == 1


def test_udf_dummy():
    lf = pl.LazyFrame(
        {
            "a": ["1", "2", "3", "4", "5"] * 5,
        }
    )

    df = lf.collect()
    assert df.shape == (25, 1)
    logger.error(f"Before udf: {df}")
    lf = lf.with_columns(dummy("a"))
    df = lf.collect()
    assert df.shape == (25, 1)
    logger.error(f"After udf: {df}")
