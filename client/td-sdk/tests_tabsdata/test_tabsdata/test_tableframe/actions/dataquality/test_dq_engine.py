#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    dataset,
    is_not_null_checks,
    is_not_null_classifiers,
    is_null_checks,
    is_null_classifiers,
)

from tabsdata.tableframe._actions.dataquality.engine import DataQualityEngine
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
    classifiers = is_null_classifiers()
    dq = tf._dq
    for classifier in classifiers:
        dq = dq.is_null(classifier.cols, classifier.tags)
    df = dq.tf()._lf.collect()
    assert isinstance(dq, DataQualityEngine)
    is_null_checks(df, dq._tags)

    dq = dq.summary(["missing_single", "missing_list"])
    df = dq.tf()._lf.collect()
    print(df)


@pytest.mark.dq
def test_is_not_null():
    tf = dataset(rows=1024)
    classifier = is_not_null_classifiers()
    dq = tf._dq.is_not_null(classifier.cols, classifier.tags)
    df = dq.tf()._lf.collect()
    assert isinstance(dq, DataQualityEngine)
    is_not_null_checks(df, dq._tags)

    dq = dq.summary(["not_missing"])
    df = dq.tf()._lf.collect()
    print(df)
