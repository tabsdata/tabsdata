#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    dataset,
)

from tabsdata.tableframe._actions.dataquality.engine import DataQualityEngine
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()
pl.Config.set_verbose(False)


def dq_parameters():
    parameters = []
    for classifier, config in CLASSIFIERS.items():
        for dtype in config.dtypes:
            parameters.append(
                pytest.param(
                    classifier,
                    dtype,
                    config,
                    id=f"{classifier.__name__}_{dtype.__name__}",
                )
            )
    return parameters


@pytest.mark.dq
@pytest.mark.parametrize("classifier,dtype,configuration", dq_parameters())
def test_dq_engine(classifier, dtype, configuration):
    tf = dataset(dtype)
    dq_engine = tf._dq
    for classifier in configuration.classifiers:
        dq_engine = configuration.method(dq_engine, classifier)
    assert isinstance(dq_engine, DataQualityEngine)
    df = dq_engine.tf()._lf.collect()
    configuration.checker(df, dq_engine._tags)
    assert "value" in df.columns
    assert "id" in df.columns
