#
# Copyright 2025 Tabs Data Inc.
#

import math

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    DATASET_ROWS,
    dataset,
)

from tabsdata.dataquality import IsIn, IsNotIn
from tabsdata.tableframe._actions.dataquality.engine import DataQualityEngine
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()
pl.Config.set_verbose(False)


def _convert_classifier_values(classifier, dtype):
    if not isinstance(classifier, (IsIn, IsNotIn)):
        return classifier
    if dtype not in DATASET_ROWS:
        return classifier

    converter = DATASET_ROWS[dtype].converter
    converted_values = set()
    for value in classifier.values:
        if value is None:
            converted_values.add(None)
        elif isinstance(value, float) and math.isnan(value):
            converted_values.add(value)
        else:
            converted_values.add(converter(value))

    if isinstance(classifier, IsIn):
        return IsIn(
            column_names=classifier.column_names,
            tags=classifier.tags,
            values=frozenset(converted_values),
        )
    else:
        return IsNotIn(
            column_names=classifier.column_names,
            tags=classifier.tags,
            values=frozenset(converted_values),
        )


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
def test_dq_engine(classifier, dtype, configuration):
    tf = dataset(dtype)
    dq_engine = tf._dq
    for classifier in configuration.classifiers:
        converted_classifier = _convert_classifier_values(classifier, dtype)
        dq_engine = configuration.method(dq_engine, converted_classifier)
    assert isinstance(dq_engine, DataQualityEngine)
    df = dq_engine.tf()._lf.collect()
    configuration.checker(df, dq_engine._tags)
    assert "value" in df.columns
    assert "id" in df.columns
