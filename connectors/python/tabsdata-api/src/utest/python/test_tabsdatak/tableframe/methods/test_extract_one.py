#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.column.connector import name_of
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe._engine.event.error import (
    EmptyFrameError,
    MultipleRowsError,
)

pytestmark = pytest.mark.tableframe

CONNECTOR_COLUMN = name_of("source")


def _frame(data: dict, schema: dict | None = None) -> TableFrame:
    return TableFrame.from_polars(pl.LazyFrame(data, schema=schema))


def _reserved_frame(data: dict) -> TableFrame:
    return TableFrame._wrap(pl.LazyFrame(data))


def test_extract_one_returns_the_only_row():
    frame = _frame({"a": [1], "b": ["x"]})
    assert frame.extract_one() == {"a": 1, "b": "x"}


def test_extract_one_returns_none_when_empty():
    frame = _frame({"a": [], "b": []}, schema={"a": pl.Int64, "b": pl.Utf8})
    assert frame.extract_one() is None


def test_extract_one_fails_when_empty_and_fail_if_empty():
    frame = _frame({"a": []}, schema={"a": pl.Int64})
    with pytest.raises(EmptyFrameError):
        frame.extract_one(fail_if_empty=True)


def test_extract_one_fails_with_more_than_one_row():
    frame = _frame({"a": [1, 2]})
    with pytest.raises(MultipleRowsError):
        frame.extract_one()


def test_extract_one_excludes_system_columns():
    frame = _frame({"a": [1]})
    assert frame.extract_one() == {"a": 1}


def test_extract_one_drops_internal_and_connector_columns_by_default():
    frame = _reserved_frame({
        "a": [1],
        CONNECTOR_COLUMN: ["/tmp/f.csv"],
        TD_IDENTIFIER_SYSTEM_COLUMN_NAME: ["id"],
    })
    assert frame.extract_one() == {"a": 1}


def test_extract_one_keeps_connector_columns_when_requested():
    frame = _reserved_frame({
        "a": [1],
        CONNECTOR_COLUMN: ["/tmp/f.csv"],
        TD_IDENTIFIER_SYSTEM_COLUMN_NAME: ["id"],
    })
    assert frame.extract_one(with_connector_columns=True) == {
        "a": 1,
        CONNECTOR_COLUMN: "/tmp/f.csv",
    }


def test_extract_one_preserves_nulls():
    frame = _frame({"a": [None], "b": [None]}, schema={"a": pl.Int64, "b": pl.Utf8})
    assert frame.extract_one() == {"a": None, "b": None}


def test_extract_one_returns_every_column_and_type():
    frame = _frame(
        {"i": [7], "f": [1.5], "s": ["x"], "flag": [True], "n": [None]},
        schema={
            "i": pl.Int64,
            "f": pl.Float64,
            "s": pl.Utf8,
            "flag": pl.Boolean,
            "n": pl.Int64,
        },
    )
    assert frame.extract_one() == {
        "i": 7,
        "f": 1.5,
        "s": "x",
        "flag": True,
        "n": None,
    }
