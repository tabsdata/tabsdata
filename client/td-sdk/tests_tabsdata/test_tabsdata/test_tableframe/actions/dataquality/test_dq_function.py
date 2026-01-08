#
# Copyright 2025 Tabs Data Inc.
#

import pytest
from fntest import tdfnrun
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    Q_NEGATIVE,
    Q_NULL,
    Q_POSITIVE,
    Q_ZERO,
    Q_NaN,
    boolean_dataset,
    categorical_dataset,
)

import tabsdata as td
import tabsdata.dataquality as dq
from tabsdata.dataquality._classifier import LinearScale
from tabsdata.tableframe._actions.dataquality.engine import MAX_BINS


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_dq_transformer():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsNull("value"),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    _ = tdfnrun(
        function=dq_transformer,
        input_tables=boolean_dataset(td.Float32),
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_summary_boolean_classifier():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsPositive("value"),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    fnrun = tdfnrun(
        function=dq_transformer,
        input_tables=boolean_dataset(td.Float32),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    dq_table = outputs[0]
    summary_table = outputs[1]

    total_rows = len(dq_table)

    assert len(summary_table) == 1
    row = summary_table.row(0, named=True)

    assert row["dq.type"] == "boolean"

    assert row["dq.bins"] is None

    false_count = row["dq.false"]
    true_count = row["dq.true"]
    nan_count = row["dq.nan"]
    none_count = row["dq.none"]

    assert false_count is not None
    assert true_count is not None
    assert nan_count is not None
    assert none_count is not None

    assert row["dq.under"] is None
    assert row["dq.over"] is None

    assert false_count == Q_ZERO + Q_NEGATIVE
    assert true_count == Q_POSITIVE
    assert nan_count == Q_NaN
    assert none_count == Q_NULL

    assert false_count + true_count + nan_count + none_count == total_rows

    for i in range(MAX_BINS + 1):
        column_name = f"dq.p{i:03d}"
        assert (
            row[column_name] is None
        ), f"Category column {column_name} should be None for boolean classifier"


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_summary_categorizer_min_in_first_bin():
    scale = LinearScale(
        min_val=0,
        max_val=100,
        bins=10,
        min_in_first_bin=True,
    )

    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.Categorizer("value", scale=scale),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    fnrun = tdfnrun(
        function=dq_transformer,
        input_tables=categorical_dataset(scale, td.Float32),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    dq_table = outputs[0]
    summary_table = outputs[1]

    total_rows = len(dq_table)

    assert len(summary_table) == 1
    row = summary_table.row(0, named=True)

    assert row["dq.type"] == "categorical"

    assert row["dq.bins"] == 10

    assert row["dq.false"] is None
    assert row["dq.true"] is None

    underflow_count = row["dq.under"]
    overflow_count = row["dq.over"]
    nan_count = row["dq.nan"]
    none_count = row["dq.none"]
    assert underflow_count is not None
    assert overflow_count is not None
    assert nan_count is not None
    assert none_count is not None

    assert row["dq.p000"] is None, "p000 should be None when min_in_first_bin=True"

    category_sum = 0
    for i in range(1, scale.bins + 1):
        column_name = f"dq.p{i:03d}"
        count = row[column_name]
        assert count is not None, f"Category column {column_name} should have a value"
        category_sum += count

    for i in range(scale.bins + 1, MAX_BINS + 1):
        column_name = f"dq.p{i:03d}"
        assert (
            row[column_name] is None
        ), f"Category column {column_name} should be None (out of bins scope)"

    assert (
        category_sum + underflow_count + overflow_count + nan_count + none_count
        == total_rows
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_summary_categorizer_min_not_in_first_bin():
    scale = LinearScale(
        min_val=0,
        max_val=100,
        bins=10,
        min_in_first_bin=False,
    )

    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.Categorizer("value", scale=scale),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    fnrun = tdfnrun(
        function=dq_transformer,
        input_tables=categorical_dataset(scale, td.Float32),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    dq_table = outputs[0]
    summary_table = outputs[1]

    total_rows = len(dq_table)

    assert len(summary_table) == 1
    row = summary_table.row(0, named=True)

    assert row["dq.type"] == "categorical"

    assert row["dq.bins"] == 10

    assert row["dq.false"] is None
    assert row["dq.true"] is None

    underflow_count = row["dq.under"]
    overflow_count = row["dq.over"]
    nan_count = row["dq.nan"]
    none_count = row["dq.none"]
    assert underflow_count is not None
    assert overflow_count is not None
    assert nan_count is not None
    assert none_count is not None

    category_sum = 0
    for i in range(scale.bins + 1):
        column_name = f"dq.p{i:03d}"
        count = row[column_name]
        assert count is not None, f"Category column {column_name} should have a value"
        category_sum += count

    for i in range(scale.bins + 1, MAX_BINS + 1):
        column_name = f"dq.p{i:03d}"
        assert (
            row[column_name] is None
        ), f"Category column {column_name} should be None (out of bins scope)"

    assert (
        category_sum + underflow_count + overflow_count + nan_count + none_count
        == total_rows
    )
