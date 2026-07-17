#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import datetime
import random

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.event.error import EmptyFrameError

pytestmark = pytest.mark.tableframe

LARGE_ROWS = 1500
LARGE_SEED = 42


def _frame(data: dict, schema: dict | None = None) -> TableFrame:
    return TableFrame.from_polars(pl.LazyFrame(data, schema=schema))


def _oracle(data: dict, cols: list[str], schema: dict | None = None) -> dict:
    lf = pl.LazyFrame(data, schema=schema)
    df = lf.bottom_k(1, by=list(cols)).select(*cols).collect()
    return df.row(0, named=True)


def test_min_for_returns_none_when_empty():
    frame = _frame({"a": []}, schema={"a": pl.Int64})
    assert frame.min_for("a") is None


def test_min_for_fails_when_empty_and_fail_if_empty():
    frame = _frame({"a": []}, schema={"a": pl.Int64})
    with pytest.raises(EmptyFrameError):
        frame.min_for("a", fail_if_empty=True)


def test_min_for_requires_a_column():
    frame = _frame({"a": [1, 2]})
    with pytest.raises(ValueError):
        frame.min_for()


def test_min_for_returns_only_the_keyed_columns():
    frame = _frame({"a": [3, 1, 2], "b": ["x", "y", "z"]})
    assert frame.min_for("a") == {"a": 1}


def test_min_for_returns_all_keyed_columns():
    frame = _frame({"a": [3, 1, 2], "b": ["x", "y", "z"], "c": [7, 8, 9]})
    assert frame.min_for("a", "c") == {"a": 1, "c": 8}


def test_min_for_breaks_ties_on_later_columns():
    frame = _frame({"a": [1, 1, 2], "b": [9, 5, 1], "c": ["x", "y", "z"]})
    assert frame.min_for("a", "b") == {"a": 1, "b": 5}


def test_min_for_ignores_nulls_in_the_keyed_column():
    frame = _frame({"a": [3, None, 1, 2], "b": ["w", "x", "y", "z"]})
    assert frame.min_for("a") == {"a": 1}


def test_min_for_with_an_all_null_keyed_column():
    frame = _frame({"a": [None, None, None]}, schema={"a": pl.Int64})
    assert frame.min_for("a") == {"a": None}


def _large_data() -> dict:
    chooser = random.Random(LARGE_SEED)
    base = datetime.date(2000, 1, 1)
    return {
        "signed": [chooser.randint(-1000, 1000) for _ in range(LARGE_ROWS)],
        "unsigned": [chooser.randint(0, 5000) for _ in range(LARGE_ROWS)],
        "floating": [chooser.uniform(-1.0, 1.0) for _ in range(LARGE_ROWS)],
        "flag": [chooser.choice([True, False]) for _ in range(LARGE_ROWS)],
        "label": [chooser.choice(["a", "b", "c", "d"]) for _ in range(LARGE_ROWS)],
        "when": [
            base + datetime.timedelta(days=chooser.randint(0, 3650))
            for _ in range(LARGE_ROWS)
        ],
        "maybe": [
            chooser.choice([None, chooser.randint(0, 10)]) for _ in range(LARGE_ROWS)
        ],
    }


def _large_schema() -> dict:
    return {
        "signed": pl.Int32,
        "unsigned": pl.UInt32,
        "floating": pl.Float64,
        "flag": pl.Boolean,
        "label": pl.Utf8,
        "when": pl.Date,
        "maybe": pl.Int64,
    }


@pytest.mark.parametrize(
    "cols",
    [
        ["signed"],
        ["label", "signed"],
        ["flag", "unsigned", "floating"],
        ["when", "signed"],
        ["maybe", "signed"],
        ["maybe"],
    ],
)
def test_min_for_matches_polars_on_a_large_frame(cols):
    data = _large_data()
    schema = _large_schema()
    frame = _frame(data, schema=schema)
    assert frame.min_for(*cols) == _oracle(data, cols, schema=schema)
