#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

import tabsdata as td
from tabsdata.extensions._tableframe.extension import SystemColumns
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()


def create_test_tableframe(column_names: list[str]) -> td.TableFrame:
    data = {name: [1, 2, 3] for name in column_names}
    lf = pl.LazyFrame(data)
    return td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )


@pytest.mark.dq
def test_name_simple_default_postfix():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = tf_i._dq.is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 1

    assert "age" in df_o.columns
    assert "name" in df_o.columns
    assert "age_dq" in df_o.columns


@pytest.mark.dq
def test_name_custom_postfix():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = tf_i._dq.with_postfix("_check").is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 1

    assert "age" in df_o.columns
    assert "name" in df_o.columns
    assert "age_check" in df_o.columns


@pytest.mark.dq
def test_name_explicit_dq_column_name():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = tf_i._dq.is_null("age", dq_column_name="age_is_missing").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 1

    assert "age" in df_o.columns
    assert "name" in df_o.columns
    assert "age_is_missing" in df_o.columns


@pytest.mark.dq
def test_name_collision_in_dq_namespace():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = tf_i._dq.is_null("age").is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 2

    assert "age" in df_o.columns
    assert "name" in df_o.columns

    assert "age_dq" in df_o.columns
    assert "age1_dq" in df_o.columns


@pytest.mark.dq
def test_name_collision_three_times():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = tf_i._dq.is_null("age").is_null("age").is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 3

    assert "age" in df_o.columns
    assert "name" in df_o.columns

    assert "age_dq" in df_o.columns
    assert "age1_dq" in df_o.columns
    assert "age2_dq" in df_o.columns


@pytest.mark.dq
def test_name_collision_with_explicit_name():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = (
        tf_i._dq.is_null("age", dq_column_name="result")
        .is_null("name", dq_column_name="result")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 2

    assert "age" in df_o.columns
    assert "name" in df_o.columns

    assert "result" in df_o.columns
    assert "result1" in df_o.columns


@pytest.mark.dq
def test_name_collision_with_existing_data_column():
    tf_i = create_test_tableframe(["age", "age_dq"])
    tf_o = tf_i._dq.is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 1

    assert "age" in df_o.columns
    assert "age_dq" in df_o.columns

    assert "age1_dq" in df_o.columns


@pytest.mark.dq
def test_name_collision_with_gaps_in_counters():
    tf_i = create_test_tableframe(["name", "name1", "name5"])
    tf_o = tf_i._dq.is_null("name").is_null("name").is_null("name").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 3

    assert "name" in df_o.columns
    assert "name1" in df_o.columns
    assert "name5" in df_o.columns

    assert "name_dq" in df_o.columns
    assert "name1_dq" in df_o.columns
    assert "name2_dq" in df_o.columns


@pytest.mark.dq
def test_name_collision_with_existing_dq_pattern_columns():
    tf_i = create_test_tableframe(["name", "name_dq", "name3_dq"])
    tf_o = tf_i._dq.is_null("name").is_null("name").is_null("name").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 3

    assert "name" in df_o.columns
    assert "name_dq" in df_o.columns
    assert "name3_dq" in df_o.columns

    assert "name1_dq" in df_o.columns
    assert "name2_dq" in df_o.columns
    assert "name4_dq" in df_o.columns


@pytest.mark.dq
def test_name_collision_complex_scenario():
    tf_i = create_test_tableframe(
        ["name", "name_dq", "name1_dq", "name2", "name3_dq", "name5_dq"]
    )
    tf_o = tf_i._dq.is_null("name").is_null("name").is_null("name").is_null("name").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 4

    assert "name" in df_o.columns
    assert "name_dq" in df_o.columns
    assert "name1_dq" in df_o.columns
    assert "name2" in df_o.columns
    assert "name3_dq" in df_o.columns
    assert "name5_dq" in df_o.columns

    assert "name2_dq" in df_o.columns
    assert "name4_dq" in df_o.columns
    assert "name6_dq" in df_o.columns
    assert "name7_dq" in df_o.columns


@pytest.mark.dq
def test_name_explicit_collision_with_data_column():
    tf_i = create_test_tableframe(["age", "result"])
    tf_o = tf_i._dq.is_null("age", dq_column_name="result").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 1

    assert "age" in df_o.columns
    assert "result" in df_o.columns

    assert "result1" in df_o.columns


@pytest.mark.dq
def test_name_explicit_collision_multiple_times():
    tf_i = create_test_tableframe(["age", "name", "check"])
    tf_o = (
        tf_i._dq.is_null("age", dq_column_name="check")
        .is_null("name", dq_column_name="check")
        .is_null("age", dq_column_name="check")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 3

    assert "age" in df_o.columns
    assert "name" in df_o.columns
    assert "check" in df_o.columns

    assert "check1" in df_o.columns
    assert "check2" in df_o.columns
    assert "check3" in df_o.columns


@pytest.mark.dq
def test_name_mixed_postfix_and_explicit():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = (
        tf_i._dq.is_null("age")
        .is_null("name", dq_column_name="name_check")
        .is_null("age")
        .is_null("name", dq_column_name="name_check")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 4

    assert "age" in df_o.columns
    assert "name" in df_o.columns

    assert "age_dq" in df_o.columns
    assert "age1_dq" in df_o.columns
    assert "name_check" in df_o.columns
    assert "name_check1" in df_o.columns


@pytest.mark.dq
def test_name_custom_postfix_with_collision():
    tf_i = create_test_tableframe(["age", "age_check"])
    tf_o = tf_i._dq.with_postfix("_check").is_null("age").is_null("age").tf()
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 2

    assert "age" in df_o.columns
    assert "age_check" in df_o.columns

    assert "age1_check" in df_o.columns
    assert "age2_check" in df_o.columns


@pytest.mark.dq
def test_name_changing_postfix_midstream():
    tf_i = create_test_tableframe(["age", "name"])
    tf_o = (
        tf_i._dq.is_null("age")
        .with_postfix("_check")
        .is_null("name")
        .is_null("age")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 3

    assert "age" in df_o.columns
    assert "name" in df_o.columns

    assert "age_dq" in df_o.columns
    assert "name_check" in df_o.columns
    assert "age_check" in df_o.columns


@pytest.mark.dq
def test_name_counters_persist_across_operations():
    tf_i = create_test_tableframe(["age", "name", "salary"])
    tf_o = (
        tf_i._dq.is_null("age")
        .is_null("name")
        .is_null("age")
        .is_null("salary")
        .is_null("age")
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 6

    assert "age" in df_o.columns
    assert "name" in df_o.columns
    assert "salary" in df_o.columns

    assert "age_dq" in df_o.columns
    assert "age1_dq" in df_o.columns
    assert "age2_dq" in df_o.columns
    assert "name_dq" in df_o.columns
    assert "name1_dq" in df_o.columns
    assert "salary_dq" in df_o.columns


@pytest.mark.dq
def test_name_postfix_specific_counters():
    tf_i = create_test_tableframe(["name"])
    tf_o = (
        tf_i._dq.with_postfix("_x")
        .is_null("name")
        .is_null("name")
        .with_postfix("_y")
        .is_null("name")
        .is_null("name")
        .with_postfix("_x")
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 5

    assert "name" in df_o.columns

    assert "name_x" in df_o.columns
    assert "name1_x" in df_o.columns
    assert "name2_x" in df_o.columns
    assert "name_y" in df_o.columns
    assert "name1_y" in df_o.columns


@pytest.mark.dq
def test_name_postfix_specific_counters_complex():
    tf_i = create_test_tableframe(["name", "age"])
    tf_o = (
        tf_i._dq.with_postfix("_x")
        .is_null("name")
        .is_null("age")
        .is_null("name")
        .with_postfix("_y")
        .is_null("name")
        .is_null("age")
        .is_null("name")
        .with_postfix("_x")
        .is_null("name")
        .is_null("age")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 8

    assert "name" in df_o.columns
    assert "age" in df_o.columns

    assert "name_x" in df_o.columns
    assert "name1_x" in df_o.columns
    assert "name2_x" in df_o.columns
    assert "name_y" in df_o.columns
    assert "name1_y" in df_o.columns

    assert "age_x" in df_o.columns
    assert "age1_x" in df_o.columns
    assert "age_y" in df_o.columns


@pytest.mark.dq
def test_name_postfix_counters_with_existing_columns():
    tf_i = create_test_tableframe(["name", "name_x", "name1_y"])
    tf_o = (
        tf_i._dq.with_postfix("_x")
        .is_null("name")
        .is_null("name")
        .with_postfix("_y")
        .is_null("name")
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 4

    assert "name" in df_o.columns
    assert "name_x" in df_o.columns
    assert "name1_y" in df_o.columns

    assert "name1_x" in df_o.columns
    assert "name2_x" in df_o.columns
    assert "name_y" in df_o.columns
    assert "name2_y" in df_o.columns


@pytest.mark.dq
def test_name_default_postfix_independence():
    tf_i = create_test_tableframe(["name"])
    tf_o = (
        tf_i._dq.is_null("name")
        .is_null("name")
        .with_postfix("_dq")
        .is_null("name")
        .with_postfix("_check")
        .is_null("name")
        .with_postfix(None)
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 5

    assert "name" in df_o.columns

    assert "name_dq" in df_o.columns
    assert "name1_dq" in df_o.columns
    assert "name2_dq" in df_o.columns
    assert "name3_dq" in df_o.columns
    assert "name_check" in df_o.columns


@pytest.mark.dq
def test_name_explicit_name_independent_from_postfix():
    tf_i = create_test_tableframe(["name"])
    tf_o = (
        tf_i._dq.with_postfix("_x")
        .is_null("name")
        .is_null("name")
        .is_null("name", dq_column_name="name")
        .is_null("name", dq_column_name="name")
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 5

    assert "name" in df_o.columns

    assert "name_x" in df_o.columns
    assert "name1_x" in df_o.columns
    assert "name2_x" in df_o.columns
    assert "name1" in df_o.columns


@pytest.mark.dq
def test_name_postfix_collision_with_data_columns():
    tf_i = create_test_tableframe(
        ["name", "name_x", "name1_x", "name2_x", "name_y", "name2_y"]
    )
    tf_o = (
        tf_i._dq.with_postfix("_x")
        .is_null("name")
        .is_null("name")
        .with_postfix("_y")
        .is_null("name")
        .is_null("name")
        .tf()
    )
    df_o = tf_o._lf.collect()

    assert len(df_o.columns) == len(tf_i.columns()) + len(SystemColumns) + 4

    assert "name" in df_o.columns
    assert "name_x" in df_o.columns
    assert "name1_x" in df_o.columns
    assert "name2_x" in df_o.columns
    assert "name_y" in df_o.columns
    assert "name2_y" in df_o.columns

    assert "name3_x" in df_o.columns
    assert "name4_x" in df_o.columns
    assert "name1_y" in df_o.columns
    assert "name3_y" in df_o.columns
