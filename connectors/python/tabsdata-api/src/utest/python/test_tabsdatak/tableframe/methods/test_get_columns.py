#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.column.meta import name_of
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


def test_get_columns_selects_the_user_columns_by_default():
    assert _frame().get_columns() == ["a", "b"]


def test_columns_is_get_columns_with_its_defaults():
    frame = _frame()
    assert frame.columns == frame.get_columns()


@pytest.mark.parametrize(
    ("user", "meta", "internal", "expected"),
    [
        (True, False, False, ["a", "b"]),
        (True, True, False, ["a", META, "b"]),
        (True, False, True, ["a", INTERNAL, "b"]),
        (True, True, True, ["a", META, INTERNAL, "b"]),
        (False, True, False, [META]),
        (False, False, True, [INTERNAL]),
        (False, True, True, [META, INTERNAL]),
        (False, False, False, []),
    ],
)
def test_get_columns_selects_each_kind_in_column_order(user, meta, internal, expected):
    columns = _frame().get_columns(
        with_user_columns=user,
        with_meta_columns=meta,
        with_internal_columns=internal,
    )
    assert columns == expected


def test_get_columns_of_a_built_frame_hides_its_internal_columns():
    frame = TableFrame.from_dict({"a": [1]})
    assert frame.get_columns() == ["a"]
    assert INTERNAL in frame.get_columns(with_internal_columns=True)


def test_get_columns_takes_keyword_arguments_only():
    with pytest.raises(TypeError):
        _frame().get_columns(True)
