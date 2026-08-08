#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._core.base import BaseFrame
from tabsdatak.tableframe._core.hooks import polars_of, tableframe_of
from tabsdatak.tableframe._engine.event.error import DirectConstructionError
from tabsdatak.tableframe._engine.frame.frame import EngineFrame
from tabsdatak.tableframe._engine.model.frame import TableFrameState

pytestmark = pytest.mark.tableframe


def _grant() -> dict:
    return {TableFrame._ACCESS_KEY: TableFrame._ACCESS_SECRET}


def _new(**kwargs) -> TableFrame:
    # noinspection PyTypeChecker
    return TableFrame.__new__(TableFrame, **kwargs)


def test_hierarchy_is_table_engine_base():
    assert inspect.getmro(TableFrame)[:3] == (TableFrame, EngineFrame, BaseFrame)


def test_direct_call_construction_is_blocked():
    with pytest.raises(DirectConstructionError):
        TableFrame({"a": [1, 2]})


def test_direct_new_construction_is_blocked():
    with pytest.raises(DirectConstructionError):
        _new()


def test_direct_init_without_grant_is_blocked():
    frame = _new(**_grant())
    with pytest.raises(DirectConstructionError):
        frame.__init__({"a": [1]})


def test_granted_construction_builds_a_tableframe():
    frame = TableFrame({"a": [1, 2, 3]}, **_grant())
    assert isinstance(frame, TableFrame)
    assert polars_of(frame, pl.LazyFrame).collect().shape == (3, 1)


def test_granted_construction_strips_the_grant_from_polars():
    frame = TableFrame({"a": [1]}, **_grant())
    assert "a" in polars_of(frame, pl.LazyFrame).collect_schema().names()


def test_granted_construction_seeds_state():
    frame = TableFrame({"a": [1]}, **_grant())
    assert isinstance(frame._state, TableFrameState)


def test_tableframe_of_wraps_a_lazyframe():
    frame = tableframe_of(pl.LazyFrame({"a": [1, 2]}))
    assert isinstance(frame, TableFrame)
    assert isinstance(frame._state, TableFrameState)
    assert polars_of(frame, pl.LazyFrame).collect().shape == (2, 1)


def test_wrap_requires_the_grant():
    with pytest.raises(DirectConstructionError):
        _new()


def test_clone_carries_a_cloned_state_forward():
    frame = tableframe_of(pl.LazyFrame({"a": [1, 2, 3]}))
    derived = frame.filter(pl.col("a") > 1)
    assert isinstance(derived, TableFrame)
    assert isinstance(derived._state, TableFrameState)
    assert derived._state is not frame._state


def test_operation_returns_a_tableframe():
    frame = tableframe_of(pl.LazyFrame({"a": [1, 2, 3]}))
    out = frame.filter(pl.col("a") > 1)
    assert isinstance(out, TableFrame)
    assert out._to_polars_df().shape == (2, 1)
    assert "$td.id" in polars_of(out, pl.LazyFrame).collect_schema().names()


def test_base_frame_is_generic_and_ungated():
    frame = BaseFrame({"a": [1]})
    assert not isinstance(frame, TableFrame)
    assert frame._state is None
