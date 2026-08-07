#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe._engine.column.connector import (
    add_connector_columns,
    describe,
    dtype_of,
    name_of,
)
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.event.error import UnsupportedMetadataError
from tabsdatak.tableframe._engine.taxonomy.column import (
    Inception,
    Language,
    SystemColumnCategory,
)

pytestmark = pytest.mark.tableframe


def _frame() -> pl.LazyFrame:
    return pl.LazyFrame({"a": [1, 2]})


def test_name_prefixes_a_bare_key():
    assert name_of("source") == f"{TD_CONNECTOR_SYSTEM_COLUMN_PREFIX}source"


def test_name_leaves_an_already_prefixed_key():
    name = f"{TD_CONNECTOR_SYSTEM_COLUMN_PREFIX}source"
    assert name_of(name) == name


@pytest.mark.parametrize(
    "value,dtype",
    [
        ("x", pl.String),
        (7, pl.Int64),
        (1.5, pl.Float64),
        (True, pl.Boolean),
    ],
)
def test_value_type_decides_the_column_type(value, dtype):
    assert dtype_of(value) is dtype


def test_boolean_is_not_read_as_an_integer():
    assert dtype_of(True) is pl.Boolean


def test_unsupported_value_type_has_no_column_type():
    assert dtype_of([1, 2]) is None


@pytest.mark.parametrize(
    "value,dtype",
    [
        ("x", pl.String),
        (7, pl.Int64),
        (1.5, pl.Float64),
        (True, pl.Boolean),
    ],
)
def test_injects_a_column_of_the_value_type(value, dtype):
    frame = add_connector_columns(_frame(), {"m": value})
    schema = frame.collect_schema()
    assert schema[name_of("m")] == dtype


def test_injects_every_entry():
    metadata = {"path": "/tmp/f.csv", "line": 7, "ratio": 1.5, "ok": True}
    frame = add_connector_columns(_frame(), metadata).collect()
    assert frame.columns == ["a"] + [name_of(key) for key in metadata]


def test_injects_the_value_on_every_row():
    frame = add_connector_columns(_frame(), {"path": "/tmp/f.csv"}).collect()
    assert frame[name_of("path")].to_list() == ["/tmp/f.csv", "/tmp/f.csv"]


def test_injects_nothing_without_metadata():
    assert add_connector_columns(_frame(), None).collect().columns == ["a"]


def test_injects_nothing_for_empty_metadata():
    assert add_connector_columns(_frame(), {}).collect().columns == ["a"]


def test_keeps_a_column_that_is_already_present():
    frame = pl.LazyFrame({"a": [1], name_of("path"): ["kept"]})
    injected = add_connector_columns(frame, {"path": "/tmp/f.csv"}).collect()
    assert injected[name_of("path")].to_list() == ["kept"]


def test_unsupported_value_type_is_rejected():
    with pytest.raises(UnsupportedMetadataError):
        add_connector_columns(_frame(), {"bad": [1, 2]})


def test_described_column_carries_the_connector_profile():
    column = describe({"path": "/tmp/f.csv"})[name_of("path")]
    assert column.category is SystemColumnCategory.CONNECTOR
    assert column.language is Language.PYTHON
    assert column.inception is Inception.PROPAGATE
    assert column.default is None
    assert column.required is False
    assert column.pristine is False
    assert column.generator is None
    assert column.aggregation is None
