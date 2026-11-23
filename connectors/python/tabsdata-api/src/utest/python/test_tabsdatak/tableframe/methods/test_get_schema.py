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
from tabsdatak.tableframe.schema import Schema

pytestmark = pytest.mark.tableframe

META = name_of("source")
INTERNAL = TD_IDENTIFIER_SYSTEM_COLUMN_NAME


def _frame() -> TableFrame:
    return TableFrame._wrap(
        pl.LazyFrame({"a": [1], META: ["/tmp/f.csv"], INTERNAL: ["id"], "b": [1.5]})
    )


def test_get_schema_selects_the_user_columns_by_default():
    schema = _frame().get_schema()
    assert isinstance(schema, Schema)
    assert dict(schema) == {"a": pl.Int64, "b": pl.Float64}


def test_schema_is_get_schema_with_its_defaults():
    frame = _frame()
    assert dict(frame.schema) == dict(frame.get_schema())


@pytest.mark.parametrize(
    ("user", "meta", "internal", "expected"),
    [
        (True, True, False, {"a": pl.Int64, META: pl.String, "b": pl.Float64}),
        (True, False, True, {"a": pl.Int64, INTERNAL: pl.String, "b": pl.Float64}),
        (False, True, True, {META: pl.String, INTERNAL: pl.String}),
        (False, False, False, {}),
    ],
)
def test_get_schema_follows_the_selected_columns(user, meta, internal, expected):
    schema = _frame().get_schema(
        with_user_columns=user,
        with_meta_columns=meta,
        with_internal_columns=internal,
    )
    assert list(schema.items()) == list(expected.items())


def test_get_schema_has_no_internal_column_to_look_up_by_default():
    with pytest.raises(KeyError):
        _frame().schema[INTERNAL]
