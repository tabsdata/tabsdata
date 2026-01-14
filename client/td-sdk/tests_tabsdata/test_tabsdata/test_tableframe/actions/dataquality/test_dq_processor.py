#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    boolean_dataset,
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
            for dtype in sorted(config.dtypes, key=lambda t: t.__name__):
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
    dataset = (
        configuration.dataset if configuration.dataset is not None else boolean_dataset
    )
    tf = dataset(dtype)
    qrows = len(tf._lf.collect())
    action = configuration.actions("table00", dtype)
    dtf, schema, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None, f"Failed for {classifier.__name__} with {dtype.__name__}"
    assert cols is not None
    assert tags is not None
    assert isinstance(dtf, DerivedTableFrame)
    assert dtf.derived_tf is not None
    df = dtf.derived_tf._lf.collect()
    configuration.checker(df, tags, qrows)
    assert "value" in df.columns
    assert "id" in df.columns
