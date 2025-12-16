#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from openai.types.responses.response_reasoning_item import Summary
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    dataset,
    is_not_null_actions,
    is_not_null_checks,
    is_null_actions,
    is_null_checks,
)

from tabsdata._tabsserver.function.typing import DerivedTableFrame
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()
pl.Config.set_verbose(False)


@pytest.mark.dq
def test_is_null():
    tf = dataset(rows=4096)
    action = is_null_actions()
    dtf, cols, tags = DataQualityProcessor.compute(tf, action)
    assert dtf is not None
    assert cols is not None
    assert tags is not None
    assert isinstance(dtf, DerivedTableFrame)
    assert dtf.derived_tf is not None
    df = dtf.derived_tf._lf.collect()
    is_null_checks(df, tags)


@pytest.mark.dq
def test_is_not_null():
    tf = dataset(rows=4096)
    action = is_not_null_actions()
    dtf, cols, tags = DataQualityProcessor.compute(tf, action)
    assert dtf is not None
    assert cols is not None
    assert tags is not None
    assert isinstance(dtf, DerivedTableFrame)
    assert dtf.derived_tf is not None
    df = dtf.derived_tf._lf.collect()
    is_not_null_checks(df, tags)
