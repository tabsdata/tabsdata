#
# Copyright 2025 Tabs Data Inc.
#

import random

import polars as pl
import pytest

import tabsdata as td
from tabsdata.extensions._tableframe.extension import SystemColumns
from tabsdata.tableframe.dq.engine import DataQualityEngine
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()


def create_large_test_dataset(seed: int = 314159, rows: int = 314):
    random.seed(seed)
    data = {
        "id": list(range(rows)),
        "age": [
            random.choice([random.randint(0, 100), None]) if i % 7 != 0 else None
            for i in range(rows)
        ],
        "salary": [
            (
                random.choice([random.uniform(-50000, 200000), None, float("nan")])
                if i % 5 != 0
                else None
            )
            for i in range(rows)
        ],
        "score": [
            (
                random.choice([random.uniform(-100, 100), float("nan")])
                if i % 3 != 0
                else None
            )
            for i in range(rows)
        ],
        "rating": [random.choice([random.randint(1, 5), None]) for i in range(rows)],
        "category": [random.choice(["A", "B", "C", "D", None]) for i in range(rows)],
        "amount": [random.uniform(-1000, 1000) for i in range(rows)],
        "count": [random.randint(-50, 150) for i in range(rows)],
        "percentage": [random.uniform(0, 100) for i in range(rows)],
        "temperature": [
            random.choice([random.uniform(-50, 50), None, float("nan")])
            for i in range(rows)
        ],
        "flag": [random.choice([True, False, None]) for i in range(rows)],
        "balance": [random.choice([0, 1, -1]) for i in range(rows)],
    }
    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    return tf


def assert_dq_columns(
    tf_input: td.TableFrame, tf_output: td.TableFrame, q_dq_columns: int
):
    df_output = tf_output._lf.collect()

    q_expected_columns = len(tf_input.columns()) + len(SystemColumns) + q_dq_columns
    assert len(df_output.columns) == q_expected_columns

    output_columns = set(df_output.columns)

    for column in SystemColumns:
        assert (
            column.value in output_columns
        ), f"System column '{column}' missing from output"
    for column in tf_input.columns():
        assert (
            column in output_columns
        ), f"Original column '{column}' missing from output"


@pytest.mark.dq
def test_is_null_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_null("age")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_dq" in df.columns
    assert df["age_dq"].dtype == td.Boolean

    age_null_count = df.filter(pl.col("age").is_null()).height
    age_dq_true_count = df.filter(pl.col("age_dq") == True).height

    assert age_null_count == age_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_null_custom_name():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_null("salary", dq_column_name="salary_missing")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "salary_missing" in df.columns
    assert df["salary_missing"].dtype == td.Boolean

    salary_null_count = df.filter(pl.col("salary").is_null()).height
    salary_dq_true_count = df.filter(pl.col("salary_missing") == True).height

    assert salary_null_count == salary_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_not_null_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_not_null("age")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_dq" in df.columns
    assert df["age_dq"].dtype == td.Boolean

    age_not_null_count = df.filter(pl.col("age").is_not_null()).height
    age_dq_true_count = df.filter(pl.col("age_dq") == True).height

    assert age_not_null_count == age_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_nan_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_nan("salary")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "salary_dq" in df.columns
    assert df["salary_dq"].dtype == td.Boolean

    salary_nan_count = df.filter(pl.col("salary").is_nan()).height
    salary_dq_true_count = df.filter(pl.col("salary_dq") == True).height

    assert salary_nan_count == salary_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_not_nan_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_not_nan("score")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "score_dq" in df.columns
    assert df["score_dq"].dtype == td.Boolean

    score_not_nan_count = df.filter(pl.col("score").is_not_nan()).height
    score_dq_true_count = df.filter(pl.col("score_dq") == True).height

    assert score_not_nan_count == score_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_null_or_nan_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_null_or_nan("temperature")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "temperature_dq" in df.columns
    assert df["temperature_dq"].dtype == td.Boolean

    temperature_null_or_nan_count = df.filter(
        pl.col("temperature").is_null() | pl.col("temperature").is_nan()
    ).height
    temperature_dq_true_count = df.filter(pl.col("temperature_dq") == True).height

    assert temperature_null_or_nan_count == temperature_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_not_null_or_nan_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_not_null_or_nan("temperature")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "temperature_dq" in df.columns
    assert df["temperature_dq"].dtype == td.Boolean

    temperature_valid_count = df.filter(
        pl.col("temperature").is_not_null() & pl.col("temperature").is_not_nan()
    ).height
    temperature_dq_true_count = df.filter(pl.col("temperature_dq") == True).height

    assert temperature_valid_count == temperature_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_in_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_in("category", ["A", "B"])

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "category_dq" in df.columns
    assert df["category_dq"].dtype == td.Boolean

    category_in_count = df.filter(pl.col("category").is_in(["A", "B"])).height
    category_dq_true_count = df.filter(pl.col("category_dq") == True).height

    assert category_in_count == category_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_in_with_numeric():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_in("rating", [1, 2, 5])

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "rating_dq" in df.columns
    assert df["rating_dq"].dtype == td.Boolean

    rating_in_count = df.filter(pl.col("rating").is_in([1, 2, 5])).height
    rating_dq_true_count = df.filter(pl.col("rating_dq") == True).height

    assert rating_in_count == rating_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_positive_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_positive("amount")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "amount_dq" in df.columns
    assert df["amount_dq"].dtype == td.Boolean

    amount_positive_count = df.filter(pl.col("amount") > 0).height
    amount_dq_true_count = df.filter(pl.col("amount_dq") == True).height

    assert amount_positive_count == amount_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_positive_or_zero_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_positive_or_zero("count")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "count_dq" in df.columns
    assert df["count_dq"].dtype == td.Boolean

    count_non_negative_count = df.filter(pl.col("count") >= 0).height
    count_dq_true_count = df.filter(pl.col("count_dq") == True).height

    assert count_non_negative_count == count_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_negative_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_negative("amount")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "amount_dq" in df.columns
    assert df["amount_dq"].dtype == td.Boolean

    amount_negative_count = df.filter(pl.col("amount") < 0).height
    amount_dq_true_count = df.filter(pl.col("amount_dq") == True).height

    assert amount_negative_count == amount_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_negative_or_zero_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_negative_or_zero("count")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "count_dq" in df.columns
    assert df["count_dq"].dtype == td.Boolean

    count_non_positive_count = df.filter(pl.col("count") <= 0).height
    count_dq_true_count = df.filter(pl.col("count_dq") == True).height

    assert count_non_positive_count == count_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_zero_basic():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_zero("balance")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "balance_dq" in df.columns
    assert df["balance_dq"].dtype == td.Boolean

    balance_zero_count = df.filter(pl.col("balance") == 0).height
    balance_dq_true_count = df.filter(pl.col("balance_dq") == True).height

    assert balance_zero_count == balance_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_between_both_closed():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_between("percentage", 25.0, 75.0, closed="both")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "percentage_dq" in df.columns
    assert df["percentage_dq"].dtype == td.Boolean

    percentage_between_count = df.filter(
        pl.col("percentage").is_between(25.0, 75.0, closed="both")
    ).height
    percentage_dq_true_count = df.filter(pl.col("percentage_dq") == True).height

    assert percentage_between_count == percentage_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_between_left_closed():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_between("percentage", 25.0, 75.0, closed="left")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "percentage_dq" in df.columns
    assert df["percentage_dq"].dtype == td.Boolean

    percentage_between_count = df.filter(
        pl.col("percentage").is_between(25.0, 75.0, closed="left")
    ).height
    percentage_dq_true_count = df.filter(pl.col("percentage_dq") == True).height

    assert percentage_between_count == percentage_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_between_right_closed():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_between("percentage", 25.0, 75.0, closed="right")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "percentage_dq" in df.columns
    assert df["percentage_dq"].dtype == td.Boolean

    percentage_between_count = df.filter(
        pl.col("percentage").is_between(25.0, 75.0, closed="right")
    ).height
    percentage_dq_true_count = df.filter(pl.col("percentage_dq") == True).height

    assert percentage_between_count == percentage_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_is_between_none_closed():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_between("percentage", 25.0, 75.0, closed="none")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "percentage_dq" in df.columns
    assert df["percentage_dq"].dtype == td.Boolean

    percentage_between_count = df.filter(
        pl.col("percentage").is_between(25.0, 75.0, closed="none")
    ).height
    percentage_dq_true_count = df.filter(pl.col("percentage_dq") == True).height

    assert percentage_between_count == percentage_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_expr_boolean():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.expr((td.col("age") > 50) & (td.col("age") < 70), "age_range_check")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_range_check" in df.columns
    assert df["age_range_check"].dtype == td.Boolean

    age_count = df.filter((pl.col("age") > 50) & (pl.col("age") < 70)).height
    age_dq_true_count = df.filter(pl.col("age_range_check") == True).height

    assert age_count == age_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_expr_int_score():
    tf = create_large_test_dataset(rows=3141)
    expr = (
        pl.when(pl.col("percentage") >= 90)
        .then(100)
        .when(pl.col("percentage") >= 75)
        .then(75)
        .when(pl.col("percentage") >= 50)
        .then(50)
        .when(pl.col("percentage") >= 25)
        .then(25)
        .otherwise(0)
        .cast(td.Int8)
    )
    dq = tf.dq.expr(expr, "quality_score")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "quality_score" in df.columns
    assert df["quality_score"].dtype == td.Int8

    assert df["quality_score"].min() >= 0
    assert df["quality_score"].max() <= 100

    score_100_count = df.filter(pl.col("percentage") >= 90).height
    score_100_dq_count = df.filter(pl.col("quality_score") == 100).height
    assert score_100_count == score_100_dq_count

    score_75_count = df.filter(
        (pl.col("percentage") >= 75) & (pl.col("percentage") < 90)
    ).height
    score_75_dq_count = df.filter(pl.col("quality_score") == 75).height
    assert score_75_count == score_75_dq_count

    score_50_count = df.filter(
        (pl.col("percentage") >= 50) & (pl.col("percentage") < 75)
    ).height
    score_50_dq_count = df.filter(pl.col("quality_score") == 50).height
    assert score_50_count == score_50_dq_count

    score_25_count = df.filter(
        (pl.col("percentage") >= 25) & (pl.col("percentage") < 50)
    ).height
    score_25_dq_count = df.filter(pl.col("quality_score") == 25).height
    assert score_25_count == score_25_dq_count

    score_0_count = df.filter(pl.col("percentage") < 25).height
    score_0_dq_count = df.filter(pl.col("quality_score") == 0).height
    assert score_0_count == score_0_dq_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_expr_complex_condition():
    tf = create_large_test_dataset(rows=3141)
    expr = (
        (td.col("salary").is_not_null())
        & (td.col("salary").is_not_nan())
        & (td.col("salary") > 0)
    )
    dq = tf.dq.expr(expr, "valid_salary")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "valid_salary" in df.columns
    assert df["valid_salary"].dtype == td.Boolean

    salary_count = df.filter(
        (pl.col("salary").is_not_null())
        & (pl.col("salary").is_not_nan())
        & (pl.col("salary") > 0)
    ).height
    salary_dq_true_count = df.filter(pl.col("valid_salary") == True).height

    assert salary_count == salary_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_fn_batch_mode_boolean():
    tf = create_large_test_dataset(rows=3141)

    def batch_fn(age, salary):
        return (age.is_not_null()) & (salary.is_not_null())

    dq = tf.dq.fn(
        data_column_names=["age", "salary"],
        dq_column_dtype=td.Boolean,
        fn=batch_fn,
        fn_mode="batch",
        dq_column_name="both_present",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "both_present" in df.columns
    assert df["both_present"].dtype == td.Boolean

    column_count = df.filter(
        (pl.col("age").is_not_null()) & (pl.col("salary").is_not_null())
    ).height
    column_dq_true_count = df.filter(pl.col("both_present") == True).height

    assert column_count == column_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_fn_batch_mode_int():
    tf = create_large_test_dataset(rows=3141)

    def batch_fn(age):
        fn_df = pl.DataFrame({"age": age})
        fn_series = fn_df.select(
            pl.when(pl.col("age").is_null())
            .then(0)
            .otherwise(
                pl.when(pl.col("age") >= 65)
                .then(100)
                .when(pl.col("age") >= 50)
                .then(75)
                .when(pl.col("age") >= 30)
                .then(50)
                .when(pl.col("age") >= 18)
                .then(25)
                .otherwise(10)
            )
            .cast(td.Int8)
            .alias("age")
        )["age"]
        return fn_series

    dq = tf.dq.fn(
        data_column_names="age",
        dq_column_dtype=td.Int8,
        fn=batch_fn,
        fn_mode="batch",
        dq_column_name="age_category_score",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_category_score" in df.columns
    assert df["age_category_score"].dtype == td.Int8
    assert df["age_category_score"].min() >= 0
    assert df["age_category_score"].max() <= 100

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_fn_row_mode_boolean():
    tf = create_large_test_dataset(rows=3141)

    def row_fn(age, salary):
        if age is None or salary is None:
            return False
        if not isinstance(salary, (int, float)):
            return False
        if isinstance(salary, float) and (salary != salary):
            return False
        return (age > 30) and (salary > 50000)

    dq = tf.dq.fn(
        data_column_names=["age", "salary"],
        dq_column_dtype=td.Boolean,
        fn=row_fn,
        fn_mode="row",
        dq_column_name="senior_well_paid",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "senior_well_paid" in df.columns
    assert df["senior_well_paid"].dtype == td.Boolean

    df_manual = df.with_columns(
        pl.when(
            pl.col("age").is_null()
            | pl.col("salary").is_null()
            | pl.col("salary").is_nan()
        )
        .then(False)
        .otherwise((pl.col("age") > 30) & (pl.col("salary") > 50000))
        .alias("manual_check")
    )

    manual_true_count = df_manual.filter(pl.col("manual_check") == True).height
    dq_true_count = df_manual.filter(pl.col("senior_well_paid") == True).height

    assert manual_true_count == dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_fn_row_mode_int():
    tf = create_large_test_dataset(rows=3141)

    def row_fn(age, salary):
        if age is None or salary is None:
            return 0

        score = 0
        if age >= 18:
            score += 25
        if age >= 40:
            score += 25
        if salary > 50000:
            score += 25
        if salary > 100000:
            score += 25

        return score

    dq = tf.dq.fn(
        data_column_names=["age", "salary"],
        dq_column_dtype=td.Int8,
        fn=row_fn,
        fn_mode="row",
        dq_column_name="combined_score",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "combined_score" in df.columns
    assert df["combined_score"].dtype == td.Int8
    assert df["combined_score"].min() >= 0
    assert df["combined_score"].max() <= 100

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_fn_single_column_string():
    tf = create_large_test_dataset(rows=3141)

    def row_fn(category):
        return category == "A"

    dq = tf.dq.fn(
        data_column_names="category",
        dq_column_dtype=td.Boolean,
        fn=row_fn,
        fn_mode="row",
        dq_column_name="is_category_a",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "is_category_a" in df.columns
    assert df["is_category_a"].dtype == td.Boolean

    column_count = df.filter(pl.col("category") == "A").height
    column_dq_true_count = df.filter(pl.col("is_category_a") == True).height

    assert column_count == column_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_chaining_multiple_operations():
    tf = create_large_test_dataset(rows=3141)

    dq = (
        tf.dq.is_null("age")
        .is_nan("salary")
        .is_positive("amount")
        .is_between("percentage", 0, 100)
        .is_in("category", ["A", "B"])
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_dq" in df.columns
    assert "salary_dq" in df.columns
    assert "amount_dq" in df.columns
    assert "percentage_dq" in df.columns
    assert "category_dq" in df.columns

    assert_dq_columns(tf, dq_tf, 5)


@pytest.mark.dq
def test_chaining_with_postfix():
    tf = create_large_test_dataset(rows=3141)

    dq = (
        tf.dq.with_postfix("_check")
        .is_null("age")
        .is_positive("amount")
        .is_in("category", ["A", "B"])
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_check" in df.columns
    assert "amount_check" in df.columns
    assert "category_check" in df.columns

    assert_dq_columns(tf, dq_tf, 3)


@pytest.mark.dq
def test_chaining_with_custom_names():
    tf = create_large_test_dataset(rows=3141)

    dq = (
        tf.dq.is_null("age", dq_column_name="age_missing")
        .is_positive("salary", dq_column_name="salary_positive")
        .is_in("category", ["A", "B"], dq_column_name="category_ab")
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_missing" in df.columns
    assert "salary_positive" in df.columns
    assert "category_ab" in df.columns

    assert_dq_columns(tf, dq_tf, 3)


@pytest.mark.dq
def test_large_dataset_performance():
    tf = create_large_test_dataset(rows=314159)

    dq = (
        tf.dq.is_null("age")
        .is_not_null("salary")
        .is_nan("score")
        .is_not_nan("temperature")
        .is_null_or_nan("salary")
        .is_not_null_or_nan("score")
        .is_positive("amount")
        .is_negative("count")
        .is_zero("amount")
        .is_between("percentage", 10, 90)
        .is_in("category", ["A", "B", "C"])
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert df.height == 314159
    assert "age_dq" in df.columns
    assert "salary_dq" in df.columns
    assert "score_dq" in df.columns

    assert_dq_columns(tf, dq_tf, 11)


@pytest.mark.dq
def test_postfix_state_persistence():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.with_postfix("_v1")

    assert isinstance(dq, DataQualityEngine)
    assert dq.postfix == "_v1"

    dq = dq.is_null("age")

    assert isinstance(dq, DataQualityEngine)
    assert dq.postfix == "_v1"

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_v1" in df.columns

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_with_no_postfix():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.with_postfix(None).is_null("age")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_dq" in df.columns

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_multiple_checks_same_column():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_null("age").is_positive("age").is_between("age", 18, 65)

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "age_dq" in df.columns
    assert "age1_dq" in df.columns
    assert "age2_dq" in df.columns

    assert_dq_columns(tf, dq_tf, 3)


@pytest.mark.dq
def test_edge_case_empty_collection():
    tf = create_large_test_dataset(rows=3141)
    dq = tf.dq.is_in("category", [])

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "category_dq" in df.columns
    assert df.filter(pl.col("category_dq") == True).height == 0

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_edge_case_all_nulls():
    data = {
        "all_null_col": [None] * 3141,
    }
    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    dq = tf.dq.is_null("all_null_col")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert df.filter(pl.col("all_null_col_dq") == True).height == 3141

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_edge_case_all_nans():
    data = {
        "all_nan_col": [float("nan")] * 3141,
    }
    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    dq = tf.dq.is_nan("all_nan_col")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert df.filter(pl.col("all_nan_col_dq") == True).height == 3141

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_mixed_data_types_validations():
    tf = create_large_test_dataset(rows=3141)

    dq = (
        tf.dq.is_null("category")
        .is_positive("amount")
        .is_in("rating", [1, 2, 3, 4, 5])
        .is_between("percentage", 0, 100)
        .is_nan("temperature")
    )
    dq_tf = dq.tf()

    assert isinstance(dq, DataQualityEngine)

    df = dq_tf._lf.collect()

    assert df.height == 3141
    for column in [
        "category_dq",
        "amount_dq",
        "rating_dq",
        "percentage_dq",
        "temperature_dq",
    ]:
        assert column in df.columns
        assert df[column].dtype == td.Boolean

    assert_dq_columns(tf, dq_tf, 5)


@pytest.mark.dq
def test_fn_with_nan_handling():
    tf = create_large_test_dataset(rows=3141)

    def batch_fn(score):
        return score.is_not_nan() & score.is_not_null()

    dq = tf.dq.fn(
        data_column_names="score",
        dq_column_dtype=td.Boolean,
        fn=batch_fn,
        fn_mode="batch",
        dq_column_name="valid_score",
    )

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "valid_score" in df.columns
    assert df["valid_score"].dtype == td.Boolean

    column_count = df.filter(
        (pl.col("score").is_not_nan()) & (pl.col("score").is_not_null())
    ).height
    column_dq_true_count = df.filter(pl.col("valid_score") == True).height

    assert column_count == column_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_expr_with_multiple_columns():
    tf = create_large_test_dataset(rows=3141)
    expr = (
        (td.col("age").is_not_null())
        & (td.col("salary").is_not_null())
        & (td.col("age") >= 18)
        & (td.col("salary") >= 30000)
    )
    dq = tf.dq.expr(expr, "eligible")

    assert isinstance(dq, DataQualityEngine)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert "eligible" in df.columns
    assert df["eligible"].dtype == td.Boolean

    column_count = df.filter(
        (pl.col("age").is_not_null())
        & (pl.col("salary").is_not_null())
        & (pl.col("age") >= 18)
        & (pl.col("salary") >= 30000)
    ).height
    column_dq_true_count = df.filter(pl.col("eligible") == True).height

    assert column_count == column_dq_true_count

    assert_dq_columns(tf, dq_tf, 1)


@pytest.mark.dq
def test_deterministic_dataset():
    tf1 = create_large_test_dataset(seed=42, rows=314159)
    tf2 = create_large_test_dataset(seed=42, rows=314159)

    df1 = tf1._lf.collect()
    df2 = tf2._lf.collect()

    data_columns = [
        "id",
        "age",
        "salary",
        "score",
        "rating",
        "category",
        "amount",
        "count",
        "percentage",
        "temperature",
        "flag",
        "balance",
    ]

    assert df1.select(data_columns).equals(df2.select(data_columns))


@pytest.mark.dq
def test_different_seeds_produce_different_data():
    tf1 = create_large_test_dataset(seed=314, rows=314159)
    tf2 = create_large_test_dataset(seed=271, rows=314159)

    df1 = tf1._lf.collect()
    df2 = tf2._lf.collect()

    data_columns = [
        "id",
        "age",
        "salary",
        "score",
        "rating",
        "category",
        "amount",
        "count",
        "percentage",
        "temperature",
        "flag",
        "balance",
    ]

    assert not df1.select(data_columns).equals(df2.select(data_columns))


@pytest.mark.dq
def test_very_large_dataset():
    tf = create_large_test_dataset(rows=3141592)

    dq = tf.dq.is_null("age").is_positive("salary").is_between("percentage", 25, 75)

    dq_tf = dq.tf()
    df = dq_tf._lf.collect()

    assert df.height == 3141592
    assert "age_dq" in df.columns
    assert "salary_dq" in df.columns
    assert "percentage_dq" in df.columns

    assert_dq_columns(tf, dq_tf, 3)


@pytest.fixture
def tf_for_expression_validation():
    lf = pl.LazyFrame(
        {
            "x1": [1, 2, 3],
            "x2": [4, 5, 6],
            "y": [7, 8, 9],
            "amount": [10.5, 20.0, 30.5],
        }
    )
    return td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )


@pytest.mark.dq
class TestExprColumnValidation:

    def test_single_column_simple(self, tf_for_expression_validation):
        dq = tf_for_expression_validation.dq.expr(pl.col("x1"), "column")
        assert "column" in dq.tf().schema.names()

    def test_single_column_with_operation(self, tf_for_expression_validation):
        dq = tf_for_expression_validation.dq.expr(pl.col("x1") + pl.col("x2"), "column")
        assert "column" in dq.tf().schema.names()

    def test_single_column_boolean_expression(self, tf_for_expression_validation):
        dq = tf_for_expression_validation.dq.expr(pl.col("amount") > 15, "column")
        assert "column" in dq.tf().schema.names()

    def test_single_column_regex_match(self, tf_for_expression_validation):
        dq = tf_for_expression_validation.dq.expr(pl.col("^amount$"), "column")
        assert "column" in dq.tf().schema.names()

    def test_multiple_columns_wildcard_all(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.all(), "column")

    def test_multiple_columns_wildcard_star(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.col("*"), "column")

    def test_multiple_columns_list(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.col(["x1", "x2"]), "column")

    def test_multiple_columns_regex(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.col("^x.*$"), "column")

    def test_multiple_columns_exclude(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.exclude("y"), "column")

    def test_zero_columns_exclude_all(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.exclude("*"), "column")

    def test_zero_columns_nonexistent_regex(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="must resolve to exactly one column"):
            tf_for_expression_validation.dq.expr(pl.col("^nonexistent.*$"), "column")

    def test_error_message_shows_column_count(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match="resolved to 2 columns"):
            tf_for_expression_validation.dq.expr(pl.col(["x1", "x2"]), "column")

    def test_error_message_shows_column_names(self, tf_for_expression_validation):
        with pytest.raises(ValueError, match=r"\['x1', 'x2'\]"):
            tf_for_expression_validation.dq.expr(pl.col(["x1", "x2"]), "column")
