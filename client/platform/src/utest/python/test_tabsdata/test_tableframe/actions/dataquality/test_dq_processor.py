#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

import tabsdata as td
import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata._tabsserver.function.typing import DerivedTableFrame
from tabsdata.dataquality import DataQuality, Enrich, IsBetween, IsIn, Summary
from tabsdata.exceptions import DataQualityProcessingError, ErrorCode
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    boolean_dataset,
)
from test_tabsdata.test_tableframe.common import (
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


def wrong_value_dataset() -> td.TableFrame:
    schema = {
        "id": pl.Int64,
        "int_col": td_dtypes.Int32,
        "float_col": td_dtypes.Float64,
        "str_col": td_dtypes.String,
    }
    data = {
        "id": list(range(10)),
        "int_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        "str_col": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    }
    df = pl.DataFrame(data, schema=schema, strict=False)
    lf = df.lazy()
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    return tf


@pytest.mark.dq
def test_on_wrong_value_ignore():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsBetween(
                column_names=["int_col"],
                min_val="invalid_string",
                max_val="more_invalid_string",
                on_wrong_value="ignore",
            ),
        ],
        operators=[Summary()],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None
    df = dtf.derived_tf._lf.collect()
    assert "int_col_is_between" not in df.columns
    assert "int_col" in df.columns


@pytest.mark.dq
def test_on_wrong_value_fail():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsBetween(
                column_names=["int_col"],
                min_val="invalid_string",
                max_val="more_invalid_string",
                on_wrong_value="fail",
            ),
        ],
        operators=[Summary()],
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        DataQualityProcessor.compute([tf], action)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "int_col" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_is_in_ignore():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsIn(
                column_names=["int_col"],
                values=frozenset(["not", "numbers"]),
                on_wrong_value="ignore",
            ),
        ],
        operators=[Enrich()],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None
    df = dtf.derived_tf._lf.collect()
    assert "int_col_is_in" not in df.columns


@pytest.mark.dq
def test_on_wrong_value_is_in_fail():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsIn(
                column_names=["float_col"],
                values=frozenset(["invalid", "values"]),
                on_wrong_value="fail",
            ),
        ],
        operators=[Enrich()],
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        DataQualityProcessor.compute([tf], action)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "float_col" in exc_info.value.message
    assert "values" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_multiple_classifiers():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsBetween(
                column_names=["int_col"],
                min_val=0,
                max_val=100,
                on_wrong_value="ignore",
            ),
            IsIn(
                column_names=["int_col"],
                values=frozenset(["bad", "values"]),
                on_wrong_value="ignore",
            ),
        ],
        operators=[Summary()],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None
    df = dtf.derived_tf._lf.collect()
    assert "int_col_is_between" in df.columns
    assert "int_col_is_in" not in df.columns


@pytest.mark.dq
def test_on_wrong_value_mixed_columns():
    tf = wrong_value_dataset()
    action = DataQuality(
        table="test_table",
        classifiers=[
            IsBetween(
                column_names=["int_col", "float_col", "str_col"],
                min_val="a",
                max_val="b",
                on_wrong_value="ignore",
            ),
        ],
        operators=[Summary()],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([tf], action)
    assert dtf is not None
    df = dtf.derived_tf._lf.collect()
    assert "int_col_is_between" not in df.columns
    assert "float_col_is_between" not in df.columns
    assert "str_col_is_between" in df.columns
