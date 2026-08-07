#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._core.translate import wrap
from tabsdatak.tableframe.schema import Schema

pytestmark = pytest.mark.tableframe


def _schema() -> Schema:
    return wrap(pl.Schema({"a": pl.Int64, "b": pl.String}))


def test_to_frame_returns_a_tableframe():
    assert isinstance(_schema().to_frame(), TableFrame)


def test_to_frame_returns_an_empty_frame_with_the_schema_columns():
    frame = _schema().to_frame()
    assert frame.to_polars_lf().collect_schema().names() == ["a", "b"]


def test_to_frame_preserves_the_schema_types():
    schema = _schema().to_frame().to_polars_lf().collect_schema()
    assert schema["a"] == pl.Int64
    assert schema["b"] == pl.String


def test_to_frame_has_no_rows():
    assert _schema().to_frame().to_polars_df().height == 0


def test_to_frame_takes_no_eager_argument():
    assert "eager" not in inspect.signature(Schema.to_frame).parameters


def test_to_frame_rejects_an_eager_argument():
    with pytest.raises(TypeError):
        _schema().to_frame(eager=True)
