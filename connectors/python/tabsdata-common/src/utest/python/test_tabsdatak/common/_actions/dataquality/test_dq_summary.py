#
# Copyright 2026 Tabsdata Inc.
#

import json

import polars as pl
import pytest

from tabsdatak.common._actions.dataquality.engine import MAX_BINS, DQColumns
from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.dataquality import (
    DataQuality,
    IsNotNull,
    IsPositive,
    LinearScale,
    ScaleCategorizer,
    Summary,
)
from tabsdatak.tableframe._core.hooks import tableframe_of

pytestmark = pytest.mark.dq


def dataset():
    lf = pl.LazyFrame({
        "a": [1, 2, None, 4],
        "b": [1.0, -2.0, 3.0, 4.0],
    })
    return tableframe_of(lf)


def summary(*classifiers) -> pl.DataFrame:
    operator = Summary()
    action = DataQuality(
        table="table00",
        classifiers=list(classifiers),
        operators=[operator],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([dataset()], action)
    harvest = DataQualityProcessor.harvest(dtf, schema, cols, tags, None, operator)
    return harvest.derived.derived_tf._lf.collect()


def test_column_precedes_classifier():
    df = summary(IsPositive(["b"]))
    columns = df.columns
    assert columns.index(DQColumns.DQ_COLUMN.value) + 1 == columns.index(
        DQColumns.DQ_CLASSIFIER.value
    )
    assert columns[-1] == DQColumns.DQ_PARAMETERS.value
    assert df.schema[DQColumns.DQ_COLUMN.value] == pl.String


def test_column_is_the_classified_column_not_the_verdict_column():
    df = summary(IsNotNull(["a", "b"])).sort(DQColumns.DQ_CLASSIFIER.value)
    assert df[DQColumns.DQ_CLASSIFIER.value].to_list() == [
        "@td.dq.a_is_not_null",
        "@td.dq.b_is_not_null",
    ]
    assert df[DQColumns.DQ_COLUMN.value].to_list() == ["a", "b"]


def test_column_is_the_classified_column_for_an_aliased_verdict_column():
    df = summary(IsNotNull([("a", "a_ok")]))
    assert df[DQColumns.DQ_CLASSIFIER.value].to_list() == ["@td.dq.a_ok"]
    assert df[DQColumns.DQ_COLUMN.value].to_list() == ["a"]


def test_column_is_the_classified_column_for_a_categorizer():
    df = summary(ScaleCategorizer(["b"], scale=LinearScale(0.0, 100.0, bins=4)))
    assert df[DQColumns.DQ_CLASSIFIER.value].to_list() == ["@td.dq.b_category"]
    assert df[DQColumns.DQ_COLUMN.value].to_list() == ["b"]


def test_total_is_the_table_row_count_on_every_row():
    df = summary(IsNotNull(["a", "b"]))
    assert df.height == 2
    assert df[DQColumns.DQ_TOTAL.value].to_list() == [4, 4]
    assert df.schema[DQColumns.DQ_TOTAL.value] == pl.UInt64


def test_total_accounts_for_every_row_of_a_boolean_classifier():
    row = summary(IsPositive(["b"])).row(0, named=True)
    counted = (
        row[DQColumns.DQ_FALSE.value]
        + row[DQColumns.DQ_TRUE.value]
        + row[DQColumns.DQ_NAN.value]
        + row[DQColumns.DQ_NONE.value]
    )
    assert counted == row[DQColumns.DQ_TOTAL.value]


def test_total_accounts_for_every_row_of_a_categorizer():
    row = summary(ScaleCategorizer(["b"], scale=LinearScale(0.0, 100.0, bins=4))).row(
        0, named=True
    )
    bins = sum(row[DQColumns.column(i)] or 0 for i in range(MAX_BINS + 1))
    counted = (
        bins
        + row[DQColumns.DQ_UNDERFLOW.value]
        + row[DQColumns.DQ_OVERFLOW.value]
        + row[DQColumns.DQ_NAN.value]
        + row[DQColumns.DQ_NONE.value]
    )
    assert counted == row[DQColumns.DQ_TOTAL.value]


def test_parameters_still_carry_the_full_envelope():
    df = summary(IsPositive(["b"], tags=["nightly"]))
    parameters = json.loads(df[DQColumns.DQ_PARAMETERS.value].item())
    assert parameters["kind"] == "dqIsPositive"
    assert parameters["spec"]["column_names"] == ["b"]
    assert parameters["spec"]["tags"] == ["nightly"]
