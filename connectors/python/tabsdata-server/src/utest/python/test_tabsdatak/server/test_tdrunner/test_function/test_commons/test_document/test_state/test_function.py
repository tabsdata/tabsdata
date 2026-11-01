#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from tabsdatak.server._tdrunner.function._commons.document.state.function import (
    USER_NAMESPACE_COLUMN,
    FunctionState,
    InvalidFunctionStateError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

VALUES = {"last-modified": "2026-05-28T00:00:00Z", "min_id": 1234, "done": True}


def test_a_state_round_trips_through_its_frame():
    frame = FunctionState(VALUES).frame()
    assert FunctionState.of(frame, "state").values == VALUES


def test_an_empty_state_is_an_empty_frame():
    assert FunctionState({}).frame().width == 0
    assert FunctionState.of(pl.DataFrame(), "state").values == {}


@pytest.mark.parametrize(
    "frame, message",
    [
        (pl.DataFrame({"a": ["v"]}), USER_NAMESPACE_COLUMN),
        (pl.DataFrame({USER_NAMESPACE_COLUMN: [{"k": "1"}, {"k": "2"}]}), "2 row"),
    ],
    ids=["another column", "two rows"],
)
def test_a_frame_of_another_shape_is_refused(frame, message):
    with pytest.raises(InvalidFunctionStateError, match=message):
        FunctionState.of(frame, "state")


@pytest.mark.parametrize(
    "values, message",
    [({42: "v"}, "ContextKeySpec"), ({"k": ["a", "b"]}, "BasicTypeSpec")],
    ids=["key", "value"],
)
def test_a_state_validates_its_keys_and_values(values, message):
    with pytest.raises(InvalidFunctionStateError, match=message) as raised:
        FunctionState(values).validate()
    assert isinstance(raised.value.__cause__, ValidationError)
