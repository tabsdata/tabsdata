#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc._dictionary import LargeColumn
from tabsdatak.conn.common.sql.cdc._large import LargeColumnPolicy
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, CdcSchemaChange
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

TABLE = "s.t"


def _classifier(satype):
    if isinstance(satype, sa_types.LargeBinary):
        return "object"
    if isinstance(satype, sa_types.Text):
        return "text"
    return None


def _schema_version(classifier=_classifier):
    registry = SchemaRegistry(
        type_mapper=lambda t: t,
        large_column_classifier=classifier,
        cdc_format=CdcFormat(),
    )
    raw = (
        ("id", "int", sa_types.Integer()),
        ("body", "text", sa_types.Text()),
        ("blob", "bin", sa_types.LargeBinary()),
    )
    return registry.register_from_log(TABLE, raw)


def _change(new_values, old_values=None, schema=None) -> CdcDataChange:
    return CdcDataChange(
        operation="i",
        transaction="tx",
        sequence=1,
        timestamp=datetime(2026, 1, 1),
        table=TABLE,
        schema=schema or _schema_version(),
        new_values=new_values,
        old_values=old_values,
    )


def test_registry_records_large_columns_with_their_kind():
    schema = _schema_version()
    assert schema.large_columns == (
        LargeColumn(name="body", kind="text"),
        LargeColumn(name="blob", kind="object"),
    )


def test_object_values_are_nulled_in_new_and_old_images():
    change = _change(
        new_values={"id": 1, "body": "x", "blob": b"\x00\x01"},
        old_values={"id": 1, "body": "y", "blob": b"\x02"},
    )
    LargeColumnPolicy(max_text_length=10).apply_to(change)
    assert change.new_values == {"id": 1, "body": "x", "blob": None}
    assert change.old_values == {"id": 1, "body": "y", "blob": None}


def test_text_within_limit_is_kept():
    change = _change(new_values={"id": 1, "body": "a" * 10, "blob": None})
    LargeColumnPolicy(max_text_length=10).apply_to(change)
    assert change.new_values["body"] == "a" * 10


def test_text_over_limit_raises_cdc_20():
    change = _change(new_values={"id": 1, "body": "a" * 11, "blob": None})
    with pytest.raises(SqlCdcException) as info:
        LargeColumnPolicy(max_text_length=10).apply_to(change)
    assert "body" in str(info.value)
    assert TABLE in str(info.value)


def test_old_image_text_over_limit_raises_cdc_20():
    change = _change(
        new_values={"id": 1, "body": "ok", "blob": None},
        old_values={"id": 1, "body": "a" * 11, "blob": None},
    )
    with pytest.raises(SqlCdcException):
        LargeColumnPolicy(max_text_length=10).apply_to(change)


def test_missing_and_null_values_are_left_alone():
    change = _change(new_values={"id": 1}, old_values=None)
    LargeColumnPolicy(max_text_length=1).apply_to(change)
    assert change.new_values == {"id": 1}
    assert change.old_values is None


def test_apply_passes_every_change_through_and_skips_non_data_changes():
    data = _change(new_values={"id": 1, "body": "x", "blob": b"z"})
    schema_change = CdcSchemaChange(table=TABLE)
    out = list(LargeColumnPolicy(max_text_length=10).apply([data, schema_change]))
    assert out == [data, schema_change]
    assert data.new_values["blob"] is None


def test_apply_to_columns_nulls_objects_and_checks_text():
    schema = _schema_version()
    columns = {
        "id": [1, 2],
        "body": ["ok", "fine"],
        "blob": [b"\x00", b"\x01"],
    }
    LargeColumnPolicy(max_text_length=10).apply_to_columns(
        table=TABLE, schema=schema, columns=columns
    )
    assert columns == {"id": [1, 2], "body": ["ok", "fine"], "blob": [None, None]}


def test_apply_to_columns_raises_on_long_text():
    schema = _schema_version()
    columns = {"id": [1], "body": ["a" * 11], "blob": [None]}
    with pytest.raises(SqlCdcException):
        LargeColumnPolicy(max_text_length=10).apply_to_columns(
            table=TABLE, schema=schema, columns=columns
        )


def test_apply_to_columns_matches_the_row_path():
    schema = _schema_version()
    policy = LargeColumnPolicy(max_text_length=10)

    change = _change(new_values={"id": 1, "body": "ok", "blob": b"\x00"})
    policy.apply_to(change)

    columns = {"id": [1], "body": ["ok"], "blob": [b"\x00"]}
    policy.apply_to_columns(table=TABLE, schema=schema, columns=columns)

    assert columns == {name: [value] for name, value in change.new_values.items()}


def test_unknown_kind_raises():
    schema = _schema_version(classifier=lambda t: "weird")
    change = _change(new_values={"id": 1, "body": "x", "blob": b"z"}, schema=schema)
    with pytest.raises(ValueError, match="weird"):
        LargeColumnPolicy(max_text_length=10).apply_to(change)
