#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest
from sqlalchemy.dialects.mssql.information_schema import columns

from fntest import FnError, MockTableFrameSpec, tdfnrun
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    Q_BOOLEAN,
    Q_NEGATIVE,
    Q_NULL,
    Q_POSITIVE,
    Q_ZERO,
    Q_NaN,
    boolean_dataset,
    categorical_dataset,
    percentage,
)

import tabsdata as td
import tabsdata.dataquality as dq
from tabsdata.dataquality._classifier import LinearScale
from tabsdata.tableframe._actions.dataquality.engine import MAX_BINS


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs",
    [
        (None, 2),
        ("enrich_table", 3),
    ],
)
def test_dq_transformer(
    enrich_to_table,
    expected_outputs,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx",
    [
        (None, 2, 0),
        ("enrich_table", 3, 2),
    ],
)
def test_summary_boolean_classifier(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    dq_table = outputs[enrich_table_idx]
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
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx",
    [
        (None, 2, 0),
        ("enrich_table", 3, 2),
    ],
)
def test_summary_categorizer_min_in_first_bin(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
):
    scale = LinearScale(
        min_val=0,
        max_val=100,
        bins=10,
        use_bin_zero=False,
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    dq_table = outputs[enrich_table_idx]
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
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx",
    [
        (None, 2, 0),
        ("enrich_table", 3, 2),
    ],
)
def test_summary_categorizer_min_not_in_first_bin(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
):
    scale = LinearScale(
        min_val=0,
        max_val=100,
        bins=10,
        use_bin_zero=True,
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    dq_table = outputs[enrich_table_idx]
    summary_table = outputs[1]

    total_rows = len(dq_table)

    assert len(summary_table) == 1
    row = summary_table.row(0, named=True)

    assert row["dq.type"] == "categorical"

    assert row["dq.bins"] == 11

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
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx,summary_table_idx",
    [
        (None, 2, 0, 1),
        ("enrich_table", 3, 1, 2),
    ],
)
def test_cols_none_selects_all_user_columns(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
    summary_table_idx,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    enrich_table = outputs[enrich_table_idx]
    dq_columns = [col for col in enrich_table.columns if col.endswith("_is_null")]
    assert len(dq_columns) == 4, "Should have 4 DQ columns (one per user column)"
    assert "int_col_is_null" in enrich_table.columns
    assert "float_col_is_null" in enrich_table.columns
    assert "str_col_is_null" in enrich_table.columns
    assert "bool_col_is_null" in enrich_table.columns

    summary_table = outputs[summary_table_idx]

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
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx,summary_table_idx",
    [
        (None, 2, 0, 1),
        ("enrich_table", 3, 1, 2),
    ],
)
def test_on_missing_column_ignore(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
    summary_table_idx,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    enrich_table = outputs[enrich_table_idx]
    dq_columns = [col for col in enrich_table.columns if col.endswith("_is_null")]
    assert len(dq_columns) == 2, "Should have 2 DQ columns (missing columns ignored)"
    assert "int_col_is_null" in enrich_table.columns
    assert "float_col_is_null" in enrich_table.columns
    assert "missing_col1_is_null" not in enrich_table.columns
    assert "missing_col2_is_null" not in enrich_table.columns

    summary_table = outputs[summary_table_idx]

    assert (
        len(summary_table) == 2
    ), "Should have 2 summary rows (missing columns ignored)"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_null", "float_col_is_null"}


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table",
    [
        None,
        "enrich_table",
    ],
)
def test_on_missing_column_fail(enrich_to_table):
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
                    dq.Enrich(to_table=enrich_to_table),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    with pytest.raises(FnError) as error:
        tdfnrun(
            function=dq_transformer,
            input_tables=multi_column_dataset(),
        )

    fnrun = error.value.fnrun
    _request = fnrun.request.as_object()
    _response = fnrun.response.as_object()
    exception = fnrun.exception.as_object()
    assert exception.code == "DQPE-011"
    assert "missing_column" in exception.message
    assert "Status is not 0" in str(error.value)


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx,summary_table_idx",
    [
        (None, 2, 0, 1),
        ("enrich_table", 3, 1, 2),
    ],
)
def test_on_wrong_type_ignore(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
    summary_table_idx,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    enrich_table = outputs[enrich_table_idx]
    dq_columns = [col for col in enrich_table.columns if col.endswith("_is_positive")]
    assert len(dq_columns) == 2, "Should have 2 DQ columns (incompatible types ignored)"
    assert "int_col_is_positive" in enrich_table.columns
    assert "float_col_is_positive" in enrich_table.columns
    assert "str_col_is_positive" not in enrich_table.columns
    assert "bool_col_is_positive" not in enrich_table.columns

    summary_table = outputs[summary_table_idx]

    assert (
        len(summary_table) == 2
    ), "Should have 2 summary rows (incompatible types ignored)"

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_positive", "float_col_is_positive"}


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table",
    [
        None,
        "enrich_table",
    ],
)
def test_on_wrong_type_fail(enrich_to_table):
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
                    dq.Enrich(to_table=enrich_to_table),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    with pytest.raises(FnError) as error:
        tdfnrun(
            function=dq_transformer,
            input_tables=multi_column_dataset(),
        )

    fnrun = error.value.fnrun
    _request = fnrun.request.as_object()
    _response = fnrun.response.as_object()
    exception = fnrun.exception.as_object()
    assert exception.code == "DQPE-012"
    assert "str_col" in exception.message
    assert "Status is not 0" in str(error.value)


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "enrich_to_table,expected_outputs,enrich_table_idx,summary_table_idx",
    [
        (None, 2, 0, 1),
        ("enrich_table", 3, 1, 2),
    ],
)
def test_combined_none_cols_with_mixed_types(
    enrich_to_table,
    expected_outputs,
    enrich_table_idx,
    summary_table_idx,
):
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
                    dq.Enrich(to_table=enrich_to_table),
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
    assert len(outputs) == expected_outputs

    enrich_table = outputs[enrich_table_idx]
    dq_columns = [col for col in enrich_table.columns if col.endswith("_is_positive")]
    assert len(dq_columns) == 2, "Should have 2 DQ columns (only compatible types)"
    assert "int_col_is_positive" in enrich_table.columns
    assert "float_col_is_positive" in enrich_table.columns

    summary_table = outputs[summary_table_idx]

    classifier_names = set(summary_table["dq.classifier"])
    assert classifier_names == {"int_col_is_positive", "float_col_is_positive"}


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "criteria,threshold,should_fail",
    [
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.RowCountThreshold(0),
            True,
            id="any_failed_rowcount_0_fail",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.RowCountThreshold(Q_BOOLEAN - Q_NULL - 1),
            True,
            id="any_failed_rowcount_below_threshold_fail",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.RowCountThreshold(Q_BOOLEAN - Q_NULL),
            False,
            id="any_failed_rowcount_at_threshold_pass",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.RowCountThreshold(Q_BOOLEAN - Q_NULL + 1),
            False,
            id="any_failed_rowcount_above_threshold_pass",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.RowCountThreshold(Q_BOOLEAN),
            False,
            id="any_failed_rowcount_100_pass",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.PercentThreshold(0),
            True,
            id="any_failed_percent_0_fail",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.PercentThreshold(93),
            True,
            id="any_failed_percent_below_threshold_fail",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.PercentThreshold(94),
            False,
            id="any_failed_percent_at_threshold_pass",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.PercentThreshold(95),
            False,
            id="any_failed_percent_above_threshold_pass",
        ),
        pytest.param(
            dq.AnyFailed(none_is_ok=False),
            dq.PercentThreshold(100),
            False,
            id="any_failed_percent_100_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.RowCountThreshold(0),
            False,
            id="all_ok_rowcount_0_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.RowCountThreshold(Q_NULL - 1),
            False,
            id="all_ok_rowcount_below_threshold_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.RowCountThreshold(Q_NULL),
            False,
            id="all_ok_rowcount_at_threshold_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.RowCountThreshold(Q_NULL + 1),
            True,
            id="all_ok_rowcount_above_threshold_fail",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.RowCountThreshold(Q_BOOLEAN),
            True,
            id="all_ok_rowcount_100_fail",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.PercentThreshold(0),
            False,
            id="all_ok_percent_0_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.PercentThreshold(percentage(Q_NULL, Q_BOOLEAN) - 1),
            False,
            id="all_ok_percent_below_threshold_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.PercentThreshold(percentage(Q_NULL, Q_BOOLEAN)),
            False,
            id="all_ok_percent_at_threshold_pass",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.PercentThreshold(percentage(Q_NULL, Q_BOOLEAN) + 1),
            True,
            id="all_ok_percent_above_threshold_fail",
        ),
        pytest.param(
            dq.AllOk(none_is_ok=False),
            dq.PercentThreshold(100),
            True,
            id="all_ok_percent_100_fail",
        ),
    ],
)
def test_fail_operation_boolean(criteria, threshold, should_fail):
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[dq.IsNull("value")],
                operators=[dq.Fail(criteria=criteria, threshold=threshold)],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    if should_fail:
        with pytest.raises(FnError) as error:
            tdfnrun(
                function=dq_transformer,
                input_tables=boolean_dataset(td.Float32),
            )
        fnrun = error.value.fnrun
        exception = fnrun.exception.as_object()
        assert exception.code == "DQPE-014"
    else:
        tdfnrun(
            function=dq_transformer,
            input_tables=boolean_dataset(td.Float32),
        )


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "criteria,threshold,should_fail",
    [
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(0),
            False,
            id="in_bins_rowcount_0_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(10),
            False,
            id="in_bins_rowcount_below_threshold_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(28),
            False,
            id="in_bins_rowcount_at_threshold_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(29),
            True,
            id="in_bins_rowcount_above_threshold_fail",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(100),
            True,
            id="in_bins_rowcount_100_fail",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(0),
            False,
            id="in_bins_percent_0_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(2),
            False,
            id="in_bins_percent_below_threshold_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(3),
            False,
            id="in_bins_percent_at_threshold_pass",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(4),
            True,
            id="in_bins_percent_above_threshold_fail",
        ),
        pytest.param(
            dq.InBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(100),
            True,
            id="in_bins_percent_100_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(0),
            True,
            id="not_in_bins_rowcount_0_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(500),
            True,
            id="not_in_bins_rowcount_below_threshold_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(802),
            False,
            id="not_in_bins_rowcount_at_threshold_pass",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(810),
            False,
            id="not_in_bins_rowcount_above_threshold_pass",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.RowCountThreshold(830),
            False,
            id="not_in_bins_rowcount_total_pass",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(0),
            True,
            id="not_in_bins_percent_0_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(90),
            True,
            id="not_in_bins_percent_below_threshold_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(96),
            True,
            id="not_in_bins_percent_96_fail",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(97),
            False,
            id="not_in_bins_percent_at_threshold_pass",
        ),
        pytest.param(
            dq.NotInBins("value_category", [1, 2, 3]),
            dq.PercentThreshold(100),
            False,
            id="not_in_bins_percent_100_pass",
        ),
    ],
)
def test_fail_operation_bins(criteria, threshold, should_fail):
    scale = LinearScale(min_val=0, max_val=100, bins=100)

    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[dq.Categorizer("value", scale=scale)],
                operators=[dq.Fail(criteria=criteria, threshold=threshold)],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    if should_fail:
        with pytest.raises(FnError) as error:
            tdfnrun(
                function=dq_transformer,
                input_tables=categorical_dataset(scale, td.Float32),
            )
        fnrun = error.value.fnrun
        exception = fnrun.exception.as_object()
        assert exception.code == "DQPE-014"
    else:
        tdfnrun(
            function=dq_transformer,
            input_tables=categorical_dataset(scale, td.Float32),
        )


def filter_select_dataset() -> td.TableFrame:
    """Dataset for testing filter and select operations with mixed data."""
    return td.TableFrame.from_polars(
        pl.DataFrame(
            {
                "id": list(range(20)),
                "value_int": [
                    -5,
                    -4,
                    -3,
                    -2,
                    -1,  # 5 negative
                    0,
                    0,
                    0,  # 3 zeros
                    1,
                    2,
                    3,
                    4,
                    5,  # 5 positive
                    None,
                    None,  # 2 nulls
                    10,
                    20,
                    30,
                    40,
                    50,  # 5 large positive
                ],
                "value_float": [
                    1.0,
                    2.0,
                    3.0,
                    4.0,
                    5.0,  # 5 in range [0-10]
                    11.0,
                    12.0,
                    13.0,
                    14.0,
                    15.0,  # 5 in range [10-20]
                    21.0,
                    22.0,
                    23.0,  # 3 in range [20-30]
                    None,
                    None,  # 2 nulls
                    -1.0,
                    -2.0,
                    -3.0,  # 3 underflow
                    101.0,
                    102.0,  # 2 overflow
                ],
            }
        )
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "operator,criteria,enrich,to_table,expected_ok_count,expected_ko_count",
    [
        pytest.param(
            "filter",
            dq.AllOk(tags="positive", none_is_ok=False),
            False,
            "failed_table",
            10,
            10,
            id="filter_bool_all_ok_positive_with_to_table",
        ),
        pytest.param(
            "filter",
            dq.AllOk(tags="positive", none_is_ok=False),
            False,
            None,
            10,
            None,
            id="filter_bool_all_ok_positive_no_to_table",
        ),
        pytest.param(
            "filter",
            dq.AnyFailed(tags="positive", none_is_ok=False),
            False,
            "failed_table",
            10,
            10,
            id="filter_bool_any_failed_with_to_table",
        ),
        pytest.param(
            "filter",
            dq.AnyFailed(tags="positive", none_is_ok=False),
            False,
            None,
            10,
            None,
            id="filter_bool_any_failed_no_to_table",
        ),
        pytest.param(
            "filter",
            dq.InBins("value_float_category", [1, 2]),
            False,
            "failed_table",
            10,
            10,
            id="filter_cat_in_bins_with_to_table",
        ),
        pytest.param(
            "filter",
            dq.InBins("value_float_category", [1, 2]),
            False,
            None,
            10,
            None,
            id="filter_cat_in_bins_no_to_table",
        ),
        pytest.param(
            "filter",
            dq.NotInBins("value_float_category", [1, 2]),
            False,
            "failed_table",
            10,
            10,
            id="filter_cat_not_in_bins_with_to_table",
        ),
        pytest.param(
            "filter",
            dq.NotInBins("value_float_category", [1, 2]),
            False,
            None,
            10,
            None,
            id="filter_cat_not_in_bins_no_to_table",
        ),
        pytest.param(
            "filter",
            dq.AllOk(tags="positive", none_is_ok=False),
            True,
            "failed_table",
            10,
            10,
            id="filter_bool_all_ok_with_enrich",
        ),
        pytest.param(
            "filter",
            dq.AnyFailed(tags="positive", none_is_ok=False),
            True,
            "failed_table",
            10,
            10,
            id="filter_bool_any_failed_with_enrich",
        ),
        pytest.param(
            "filter",
            dq.InBins("value_float_category", [1]),
            True,
            "failed_table",
            5,
            15,
            id="filter_cat_in_bins_with_enrich",
        ),
        pytest.param(
            "filter",
            dq.NotInBins("value_float_category", [1]),
            True,
            "failed_table",
            15,
            5,
            id="filter_cat_not_in_bins_with_enrich",
        ),
        pytest.param(
            "select",
            dq.AllOk(tags="positive", none_is_ok=False),
            False,
            "selected_table",
            10,
            None,
            id="select_bool_all_ok",
        ),
        pytest.param(
            "select",
            dq.AnyFailed(tags="positive", none_is_ok=False),
            False,
            "selected_table",
            10,
            None,
            id="select_bool_any_failed",
        ),
        pytest.param(
            "select",
            dq.InBins("value_float_category", [2, 3]),
            False,
            "selected_table",
            8,
            None,
            id="select_cat_in_bins",
        ),
        pytest.param(
            "select",
            dq.NotInBins("value_float_category", [2, 3]),
            False,
            "selected_table",
            12,
            None,
            id="select_cat_not_in_bins",
        ),
        pytest.param(
            "select",
            dq.AllOk(tags="positive", none_is_ok=False),
            True,
            "selected_table",
            10,
            None,
            id="select_bool_all_ok_with_enrich",
        ),
        pytest.param(
            "select",
            dq.AnyFailed(tags="positive", none_is_ok=False),
            True,
            "selected_table",
            10,
            None,
            id="select_bool_any_failed_with_enrich",
        ),
        pytest.param(
            "select",
            dq.InBins("value_float_category", [1]),
            True,
            "selected_table",
            5,
            None,
            id="select_cat_in_bins_with_enrich",
        ),
        pytest.param(
            "select",
            dq.NotInBins("value_float_category", [1]),
            True,
            "selected_table",
            15,
            None,
            id="select_cat_not_in_bins_with_enrich",
        ),
    ],
)
def test_filter_select_operations(
    operator,
    criteria,
    enrich,
    to_table,
    expected_ok_count,
    expected_ko_count,
):
    scale = LinearScale(min_val=0, max_val=30, bins=3)

    operators = None
    match operator:
        case "filter":
            operators = [dq.Filter(criteria=criteria, to_table=to_table, enrich=enrich)]
        case "select":
            operators = [dq.Select(criteria=criteria, to_table=to_table, enrich=enrich)]

    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsPositive(
                        "value_int",
                        tags="positive",
                    ),
                    dq.Categorizer(
                        "value_float",
                        scale=scale,
                    ),
                ],
                operators=operators,
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    fnrun = tdfnrun(
        function=dq_transformer,
        input_tables=filter_select_dataset(),
    )

    outputs = list(fnrun.output.as_pl_df(True))

    if to_table is None:
        expected_outputs = 1
    else:
        expected_outputs = 2

    assert (
        len(outputs) == expected_outputs
    ), f"Expected {expected_outputs} outputs, got {len(outputs)}"

    ok_table = outputs[0]

    match operator:
        case "filter":
            assert (
                len(ok_table) == expected_ok_count
            ), f"Expected {expected_ok_count} rows in ok_table, got {len(ok_table)}"
        case "select":
            assert len(ok_table) == 20, "Main output should be unchanged for select"

    if to_table is not None:
        derived_table = outputs[1]

        match operator:
            case "filter":
                assert len(derived_table) == expected_ko_count, (
                    f"Expected {expected_ko_count} rows in ko_table, got"
                    f" {len(derived_table)}"
                )
            case "select":
                assert len(derived_table) == expected_ok_count, (
                    f"Expected {expected_ok_count} rows in selected_table, got"
                    f" {len(derived_table)}"
                )

        match enrich:
            case True:
                match criteria:
                    case dq.AllOk() | dq.AnyFailed():
                        assert (
                            "value_int_is_positive" in derived_table.columns
                        ), "Enriched table should include DQ columns for BoolCriteria"
                    case dq.InBins() | dq.NotInBins():
                        assert "value_float_category" in derived_table.columns, (
                            "Enriched table should include DQ column for"
                            " CategoryCriteria"
                        )
            case False:
                dq_columns = [
                    columns
                    for columns in derived_table.columns
                    if "_is_" in columns or "_category" in columns
                ]
                assert (
                    len(dq_columns) == 0
                ), f"Non-enriched table should not have DQ columns, found: {dq_columns}"

    assert "id" in ok_table.columns
    assert "value_int" in ok_table.columns
    assert "value_float" in ok_table.columns
