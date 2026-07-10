#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._core.hooks import (
    FrameView,
    chain,
    get_hook,
    set_hook,
)

from test_tabsdatak.tableframe.support.frames import tableframe_to_eager

pytestmark = pytest.mark.tableframe


@pytest.fixture
def hooks_isolated():
    saved = dict(TableFrame.__dict__.get("_hooks", {}))
    try:
        yield
    finally:
        TableFrame._hooks = saved


def frame() -> TableFrame:
    return TableFrame.from_polars(pl.LazyFrame({"a": [1, 2, 3], "b": [10, 20, 30]}))


def test_default_is_transparent(hooks_isolated):
    result = frame().filter(pl.col("a") > 1)
    assert tableframe_to_eager(result)["a"].to_list() == [2, 3]


def test_before_injects_into_input(hooks_isolated):
    def before(lazyframes, op, views, params):
        return [lazyframe.filter(pl.col("a") > 1) for lazyframe in lazyframes]

    set_hook(TableFrame, "filter", before, phase="before")
    result = frame().filter(pl.col("b") < 30)
    assert tableframe_to_eager(result)["a"].to_list() == [2]


def test_during_owns_the_call(hooks_isolated):
    def during(call, lazyframes, op, views, params):
        return lazyframes[0].head(1)

    set_hook(TableFrame, "filter", during, phase="during")
    result = frame().filter(pl.col("a") > 1)
    assert tableframe_to_eager(result)["a"].to_list() == [1]


def test_after_appends_to_result(hooks_isolated):
    def after(lazyframe, op, views, params):
        return lazyframe.with_columns((pl.col("a") * 100).alias("a"))

    set_hook(TableFrame, "filter", after, phase="after")
    result = frame().filter(pl.col("a") > 1)
    assert tableframe_to_eager(result)["a"].to_list() == [200, 300]


def test_state_transforms_the_built_state(hooks_isolated):
    marker = object()

    def state(built, op, views, params):
        built.aspects["marker"] = marker
        return built

    set_hook(TableFrame, "filter", state, phase="state")
    result = frame().filter(pl.col("a") > 1)
    assert result._state.aspects["marker"] is marker


def test_view_is_an_immutable_snapshot(hooks_isolated):
    captured = {}

    def before(lazyframes, op, views, params):
        captured["view"] = views[0]
        return lazyframes

    set_hook(TableFrame, "filter", before, phase="before")
    source = frame()
    source.filter(pl.col("a") > 1)
    view = captured["view"]
    assert isinstance(view, FrameView)
    assert view.state is not source._state
    with pytest.raises(Exception):
        view.state = None


def test_chain_threads_the_working_value(hooks_isolated):
    def add_ten(lazyframe, op, views, params):
        return lazyframe.with_columns((pl.col("a") + 10).alias("a"))

    def times_two(lazyframe, op, views, params):
        return lazyframe.with_columns((pl.col("a") * 2).alias("a"))

    set_hook(TableFrame, "filter", chain(add_ten, times_two), phase="after")
    result = frame().filter(pl.col("a") > 0)
    assert tableframe_to_eager(result)["a"].to_list() == [22, 24, 26]


def test_get_hook_resolves_through_mro(hooks_isolated):
    def after(lazyframe, op, views, params):
        return lazyframe

    set_hook(TableFrame, "filter", after, phase="after")

    class Derived(TableFrame):
        pass

    assert get_hook(Derived, "filter", "after") is after
    assert get_hook(TableFrame, "filter", "before") is None
