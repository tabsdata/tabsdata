#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    dataset,
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


def dq_parameters():
    parameters = []
    for classifier, configs in CLASSIFIERS.items():
        for config in configs:
            for dtype in config.dtypes:
                if config.name is not None:
                    config_id = f"{classifier.__name__}_{dtype.__name__}_{config.name}"
                else:
                    config_id = f"{classifier.__name__}_{dtype.__name__}"
                parameters.append(
                    pytest.param(
                        classifier,
                        dtype,
                        config,
                        id=config_id,
                    )
                )
    return parameters


@pytest.mark.dq
@pytest.mark.parametrize("classifier,dtype,configuration", dq_parameters())
def test_dq_processor(classifier, dtype, configuration):
    tf = dataset(dtype)
    action = configuration.actions("table00", dtype)
    dtf, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None, f"Failed for {classifier.__name__} with {dtype.__name__}"
    assert cols is not None
    assert tags is not None
    assert isinstance(dtf, DerivedTableFrame)
    assert dtf.derived_tf is not None
    df = dtf.derived_tf._lf.collect()
    configuration.checker(df, tags)
    assert "value" in df.columns
    assert "id" in df.columns
