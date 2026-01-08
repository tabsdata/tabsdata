#
# Copyright 2025 Tabs Data Inc.
#

import math

import polars as pl
import pytest
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    DATASET_ROWS,
    NaN,
    boolean_dataset,
)

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata.dataquality import IsBetween, IsIn, IsNotBetween, IsNotIn
from tabsdata.tableframe._actions.dataquality.engine import DataQualityEngine
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()
pl.Config.set_verbose(False)


def _convert_classifier_values(classifier, dtype):
    if not isinstance(classifier, (IsIn, IsNotIn, IsBetween, IsNotBetween)):
        return classifier
    if dtype not in DATASET_ROWS:
        return classifier

    converter = DATASET_ROWS[dtype].converter

    is_uint = dtype in (
        td_dtypes.UInt8,
        td_dtypes.UInt16,
        td_dtypes.UInt32,
        td_dtypes.UInt64,
    )

    if isinstance(classifier, (IsIn, IsNotIn)):
        converted_values = set()
        for value in classifier.values:
            if value is None:
                converted_values.add(None)
            elif isinstance(value, float) and math.isnan(value):
                converted_values.add(value)
            elif is_uint and value == NaN:
                converted_values.add(None)
            elif is_uint and isinstance(value, (int, float)):
                converted_values.add(int(value))
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
    elif isinstance(classifier, (IsBetween, IsNotBetween)):
        if is_uint:
            if classifier.min is not None:
                if classifier.min == NaN:
                    converted_min = None
                elif isinstance(classifier.min, (int, float)):
                    converted_min = int(classifier.min)
                else:
                    converted_min = converter(classifier.min)
            else:
                converted_min = None

            if classifier.max is not None:
                if classifier.max == NaN:
                    converted_max = None
                elif isinstance(classifier.max, (int, float)):
                    converted_max = int(classifier.max)
                else:
                    converted_max = converter(classifier.max)
            else:
                converted_max = None
        else:
            converted_min = (
                converter(classifier.min) if classifier.min is not None else None
            )
            converted_max = (
                converter(classifier.max) if classifier.max is not None else None
            )

        if isinstance(classifier, IsBetween):
            return IsBetween(
                column_names=classifier.column_names,
                tags=classifier.tags,
                min_len=converted_min,
                max_len=converted_max,
                closed=classifier.closed,
            )
        else:
            return IsNotBetween(
                column_names=classifier.column_names,
                tags=classifier.tags,
                min_len=converted_min,
                max_len=converted_max,
                closed=classifier.closed,
            )
    return None


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
    dataset = (
        configuration.dataset if configuration.dataset is not None else boolean_dataset
    )
    tf = dataset(dtype)
    qrows = len(tf._lf.collect())
    dq_engine = tf._dq
    for classifier in configuration.classifiers:
        converted_classifier = _convert_classifier_values(classifier, dtype)
        dq_engine = configuration.method(dq_engine, converted_classifier)
    assert isinstance(dq_engine, DataQualityEngine)
    df = dq_engine.tf()._lf.collect()
    configuration.checker(df, dq_engine._tags, qrows)
    assert "value" in df.columns
    assert "id" in df.columns
