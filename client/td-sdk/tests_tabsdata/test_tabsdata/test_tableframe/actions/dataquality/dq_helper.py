#
# Copyright 2025 Tabs Data Inc.
#

import random

import polars as pl

import tabsdata as td
from tabsdata.dataquality import DataQuality, IsNotNull, IsNull
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder


def dataset(seed: int = 314159, rows: int = 314) -> td.TableFrame:
    random.seed(seed)
    data = {
        "id": list(range(rows)),
        "number01": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-1)
                    if i % 5 == 1
                    else (
                        float(1)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
        "number02": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-2)
                    if i % 5 == 1
                    else (
                        float(2)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
        "number03": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-3)
                    if i % 5 == 1
                    else (
                        float(3)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
        "number04": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-4)
                    if i % 5 == 1
                    else (
                        float(4)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
        "number05": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-5)
                    if i % 5 == 1
                    else (
                        float(5)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
        "number06": [
            (
                float(0)
                if i % 5 == 0
                else (
                    float(-6)
                    if i % 5 == 1
                    else (
                        float(6)
                        if i % 5 == 2
                        else (None if i % 5 == 3 else float("nan"))
                    )
                )
            )
            for i in range(rows)
        ],
    }
    lf = pl.LazyFrame(data)
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    return tf


def is_null_classifiers():
    return [
        IsNull(cols="number01", tags=["missing_single"]),
        IsNull(
            cols=("number02", "number02_missing"),
            tags=["missing_single"],
        ),
        IsNull(
            cols=["number03", "number04"],
            tags=["missing_list"],
        ),
        IsNull(
            cols=[
                ("number05", "number05_missing"),
                ("number06", "number06_missing"),
            ],
            tags=["missing_list"],
        ),
    ]


def is_null_actions(table: str = "table00"):
    return DataQuality(
        table=table,
        classify=is_null_classifiers(),
    )


def is_null_checks(df: pl.DataFrame, tags: dict):
    assert "id" in df.columns
    assert "number01" in df.columns
    assert "number02" in df.columns
    assert "number03" in df.columns
    assert "number04" in df.columns
    assert "number05" in df.columns
    assert "number06" in df.columns

    assert "number01_is_null" in df.columns
    assert "number02_missing" in df.columns
    assert "number03_is_null" in df.columns
    assert "number04_is_null" in df.columns
    assert "number05_missing" in df.columns
    assert "number06_missing" in df.columns

    assert len(tags) == 2, f"Expected 2 tags, got {len(tags)}"
    assert "missing_single" in tags
    assert "missing_list" in tags

    assert "number01_is_null" in tags["missing_single"]
    assert "number02_missing" in tags["missing_single"]
    assert (
        len(tags["missing_single"]) == 2
    ), f"Expected 2 columns in 'missing_single', got {len(tags['missing_single'])}"

    assert "number03_is_null" in tags["missing_list"]
    assert "number04_is_null" in tags["missing_list"]
    assert "number05_missing" in tags["missing_list"]
    assert "number06_missing" in tags["missing_list"]
    assert (
        len(tags["missing_list"]) == 4
    ), f"Expected 4 columns in 'missing_list', got {len(tags['missing_list'])}"

    number01_null_count = df.filter(pl.col("number01").is_null()).height
    number01_dq_true_count = df.filter(pl.col("number01_is_null") == True).height
    assert number01_null_count == number01_dq_true_count, (
        f"number01: null_count={number01_null_count},"
        f" dq_true_count={number01_dq_true_count}"
    )

    number02_null_count = df.filter(pl.col("number02").is_null()).height
    number02_dq_true_count = df.filter(pl.col("number02_missing") == True).height
    assert number02_null_count == number02_dq_true_count, (
        f"number02: null_count={number02_null_count},"
        f" dq_true_count={number02_dq_true_count}"
    )

    number03_null_count = df.filter(pl.col("number03").is_null()).height
    number03_dq_true_count = df.filter(pl.col("number03_is_null") == True).height
    assert number03_null_count == number03_dq_true_count, (
        f"number03: null_count={number03_null_count},"
        f" dq_true_count={number03_dq_true_count}"
    )

    number04_null_count = df.filter(pl.col("number04").is_null()).height
    number04_dq_true_count = df.filter(pl.col("number04_is_null") == True).height
    assert number04_null_count == number04_dq_true_count, (
        f"number04: null_count={number04_null_count},"
        f" dq_true_count={number04_dq_true_count}"
    )

    number05_null_count = df.filter(pl.col("number05").is_null()).height
    number05_dq_true_count = df.filter(pl.col("number05_missing") == True).height
    assert number05_null_count == number05_dq_true_count, (
        f"number05: null_count={number05_null_count},"
        f" dq_true_count={number05_dq_true_count}"
    )

    number06_null_count = df.filter(pl.col("number06").is_null()).height
    number06_dq_true_count = df.filter(pl.col("number06_missing") == True).height
    assert number06_null_count == number06_dq_true_count, (
        f"number06: null_count={number06_null_count},"
        f" dq_true_count={number06_dq_true_count}"
    )


def is_not_null_classifiers():
    return IsNotNull(
        cols="number01",
        tags=["not_missing"],
    )


def is_not_null_actions(table: str = "table00"):
    return DataQuality(
        table=table,
        classify=is_not_null_classifiers(),
    )


def is_not_null_checks(df: pl.DataFrame, tags: dict):
    assert "number01" in df.columns
    assert "number01_is_not_null" in df.columns

    assert "not_missing" in tags
    assert "number01_is_not_null" in tags["not_missing"]

    number01_not_null_count = df.filter(pl.col("number01").is_not_null()).height
    number01_dq_true_count = df.filter(pl.col("number01_is_not_null") == True).height
    assert number01_not_null_count == number01_dq_true_count, (
        f"number01: not_null_count={number01_not_null_count},"
        f" dq_true_count={number01_dq_true_count}"
    )
