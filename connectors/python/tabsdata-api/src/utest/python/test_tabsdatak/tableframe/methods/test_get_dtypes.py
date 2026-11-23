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
        pl.LazyFrame({"a": [1], META: ["/tmp/f.csv"], INTERNAL: ["id"], "b": [1.5]})
    )


def test_get_dtypes_selects_the_user_columns_by_default():
    assert _frame().get_dtypes() == [pl.Int64, pl.Float64]


def test_dtypes_is_get_dtypes_with_its_defaults():
    frame = _frame()
    assert frame.dtypes == frame.get_dtypes()


@pytest.mark.parametrize(
    ("user", "meta", "internal", "expected"),
    [
        (True, True, False, [pl.Int64, pl.String, pl.Float64]),
        (True, False, True, [pl.Int64, pl.String, pl.Float64]),
        (False, True, False, [pl.String]),
        (False, False, True, [pl.String]),
        (False, False, False, []),
    ],
)
def test_get_dtypes_follows_the_selected_columns(user, meta, internal, expected):
    dtypes = _frame().get_dtypes(
        with_user_columns=user,
        with_meta_columns=meta,
        with_internal_columns=internal,
    )
    assert dtypes == expected


def test_get_dtypes_matches_get_columns():
    frame = _frame()
    flags = {"with_meta_columns": True, "with_internal_columns": True}
    schema = frame._lf.collect_schema()
    assert frame.get_dtypes(**flags) == [
        schema[name] for name in frame.get_columns(**flags)
    ]
