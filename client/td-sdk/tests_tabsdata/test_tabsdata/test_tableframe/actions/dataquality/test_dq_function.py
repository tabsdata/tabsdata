#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from fntest import MockTableFrameSpec, tdfnrun
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
from tabsdata import TableFrame
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


def multi_column_dataset() -> MockTableFrameSpec:
    return td.TableFrame.from_polars(
        pl.DataFrame(
            {
                "int_col": [
                    1,
                    2,
                    3,
                    4,
                    5,
                    None,
                    7,
                    8,
                    9,
                    10,
                ],
                "float_col": [
                    1.5,
                    2.5,
                    3.5,
                    None,
                    5.5,
                    6.5,
                    7.5,
                    8.5,
                    9.5,
                    10.5,
                ],
                "str_col": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                    "f",
                    None,
                    "h",
                    "i",
                    "j",
                ],
                "bool_col": [
                    True,
                    False,
                    True,
                    False,
                    None,
                    True,
                    False,
                    True,
                    False,
                    True,
                ],
            }
        )
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_cols_none_selects_all_user_columns():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsNull(column_names=None),
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
        input_tables=multi_column_dataset(),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    # ToDo: check when select() & filter() are implemented...
    # dq_table = outputs[0]
    # dq_columns = [col for col in dq_table.columns if col.endswith("_is_null")]
    # assert len(dq_columns) == 4, "Should have 4 DQ columns (one per user column)"
    # assert "int_col_is_null" in dq_table.columns
    # assert "float_col_is_null" in dq_table.columns
    # assert "str_col_is_null" in dq_table.columns
    # assert "bool_col_is_null" in dq_table.columns

    summary_table = outputs[1]

    assert len(summary_table) == 4, "Should have 4 summary rows (one per user column)"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {
        "int_col_is_null",
        "float_col_is_null",
        "str_col_is_null",
        "bool_col_is_null",
    }


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_on_missing_column_ignore():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsNull(
                        column_names=[
                            "int_col",
                            "missing_col1",
                            "float_col",
                            "missing_col2",
                        ],
                        on_missing_column="ignore",
                    ),
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
        input_tables=multi_column_dataset(),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    # ToDo: check when select() & filter() are implemented...
    # dq_table = outputs[0]
    # dq_columns = [col for col in dq_table.columns if col.endswith("_is_null")]
    # assert len(dq_columns) == 2, "Should have 2 DQ columns (missing columns ignored)"
    # assert "int_col_is_null" in dq_table.columns
    # assert "float_col_is_null" in dq_table.columns
    # assert "missing_col1_is_null" not in dq_table.columns
    # assert "missing_col2_is_null" not in dq_table.columns

    summary_table = outputs[1]

    assert (
        len(summary_table) == 2
    ), "Should have 2 summary rows (missing columns ignored)"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_null", "float_col_is_null"}


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_on_missing_column_fail():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsNull(
                        column_names=["int_col", "missing_column"],
                        on_missing_column="fail",
                    ),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    with pytest.raises(Exception) as error:
        tdfnrun(
            function=dq_transformer,
            input_tables=multi_column_dataset(),
        )

    # ToDo: check when we propagate exception reason in executions...
    # assert "DQPE-011" in str(error.value)
    # assert "missing_column" in str(error.value)

    assert "Status is not 0" in str(error.value)


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_on_wrong_type_ignore():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsPositive(
                        column_names=["int_col", "float_col", "str_col", "bool_col"],
                        on_wrong_type="ignore",
                    ),
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
        input_tables=multi_column_dataset(),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    # ToDo: check when select() & filter() are implemented...
    # dq_table = outputs[0]
    # dq_columns = [col for col in dq_table.columns if col.endswith("_is_positive")]
    # assert len(dq_columns) == 2, "Should have 2 DQ columns (incompatible types ignored)"
    # assert "int_col_is_positive" in dq_table.columns
    # assert "float_col_is_positive" in dq_table.columns
    # assert "str_col_is_positive" not in dq_table.columns
    # assert "bool_col_is_positive" not in dq_table.columns

    summary_table = outputs[1]

    assert (
        len(summary_table) == 2
    ), "Should have 2 summary rows (incompatible types ignored)"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_positive", "float_col_is_positive"}


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_on_wrong_type_fail():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    # IsPositive doesn't support string types
                    dq.IsPositive(
                        column_names=["int_col", "str_col"],
                        on_wrong_type="fail",
                    ),
                ],
                operators=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    with pytest.raises(Exception) as error:
        tdfnrun(
            function=dq_transformer,
            input_tables=multi_column_dataset(),
        )

    # ToDo: check when we propagate exception reason in executions...
    # assert "DQPE-012" in str(exc_info.value)
    # assert "str_col" in str(exc_info.value)

    assert "Status is not 0" in str(error.value)


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_combined_none_cols_with_mixed_types():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsPositive(
                        column_names=None,
                        on_wrong_type="ignore",
                    ),
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
        input_tables=multi_column_dataset(),
    )

    outputs = list(fnrun.output.as_pl_df(True))
    assert len(outputs) == 2

    # ToDo: check when select() & filter() are implemented...
    # dq_table = outputs[0]
    # dq_columns = [col for col in dq_table.columns if col.endswith("_is_positive")]
    # assert len(dq_columns) == 2, "Should have 2 DQ columns (only compatible types)"
    # assert "int_col_is_positive" in dq_table.columns
    # assert "float_col_is_positive" in dq_table.columns

    summary_table = outputs[1]

    assert len(summary_table) == 2, "Should have 2 summary rows"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_positive", "float_col_is_positive"}
