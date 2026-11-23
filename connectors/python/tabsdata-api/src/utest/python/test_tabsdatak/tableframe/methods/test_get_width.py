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

pytestmark = pytest.mark.tableframe

META = name_of("source")
INTERNAL = TD_IDENTIFIER_SYSTEM_COLUMN_NAME


def _frame() -> TableFrame:
    return TableFrame._wrap(
        pl.LazyFrame({"a": [1], META: ["/tmp/f.csv"], INTERNAL: ["id"], "b": ["x"]})
    )


def test_get_width_counts_the_user_columns_by_default():
    assert _frame().get_width() == 2


def test_width_is_get_width_with_its_defaults():
    frame = _frame()
    assert frame.width == frame.get_width()


@pytest.mark.parametrize(
    ("user", "meta", "internal", "expected"),
    [
        (True, True, False, 3),
        (True, False, True, 3),
        (True, True, True, 4),
        (False, True, True, 2),
        (False, False, False, 0),
    ],
)
def test_get_width_counts_the_selected_columns(user, meta, internal, expected):
    width = _frame().get_width(
        with_user_columns=user,
        with_meta_columns=meta,
        with_internal_columns=internal,
    )
    assert width == expected
