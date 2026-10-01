#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import schema_hash
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

TABLE = "s.t"


def _registry() -> SchemaRegistry:
    return SchemaRegistry(
        type_mapper=lambda t: t,
        large_column_classifier=lambda t: None,
        cdc_format=CdcFormat(columns_format="columns", flatten_columns=True),
    )


def _tracked(pk: list[str]) -> TrackedTable:
    return TrackedTable(
        data_table=TableObject(schema="s", table="t"),
        cdc_table=TableObject(schema="s", table="t"),
        columns=[
            TrackedColumn(name="a", satype=sa_types.Integer()),
            TrackedColumn(name="b", satype=sa_types.Integer()),
            TrackedColumn(name="c", satype=sa_types.String()),
        ],
        pk=pk,
    )


def _raw(*names: str) -> tuple:
    return tuple((name, "INTEGER", sa_types.Integer()) for name in names)


def test_a_catalog_version_carries_the_primary_key():
    version = _registry().register_from_catalog(TABLE, _tracked(["a", "b"]))
    assert version.primary_key == ("a", "b")


def test_a_log_version_carries_the_catalog_primary_key():
    registry = _registry()
    registry.register_from_catalog(TABLE, _tracked(["a"]))
    assert registry.register_from_log(TABLE, _raw("a", "b", "x")).primary_key == ("a",)


def test_a_log_version_keeps_the_key_even_without_its_columns():
    registry = _registry()
    registry.register_from_catalog(TABLE, _tracked(["a"]))
    assert registry.register_from_log(TABLE, _raw("b", "c")).primary_key == ("a",)


def test_the_same_columns_from_the_log_are_the_catalog_version():
    registry = _registry()
    catalog = registry.register_from_catalog(TABLE, _tracked(["a"]))
    raw = (
        ("a", "integer", sa_types.Integer()),
        ("b", "int4", sa_types.Integer()),
        ("c", "character varying", sa_types.String()),
    )
    logged = registry.register_from_log(TABLE, raw)
    assert logged is catalog
    assert registry.latest(TABLE) is catalog


def test_a_column_of_another_type_is_a_new_version():
    registry = _registry()
    catalog = registry.register_from_catalog(TABLE, _tracked(["a"]))
    raw = (
        ("a", "integer", sa_types.Integer()),
        ("b", "integer", sa_types.Integer()),
        ("c", "integer", sa_types.Integer()),
    )
    logged = registry.register_from_log(TABLE, raw)
    assert logged.hash != catalog.hash
    assert logged.version == 1


def test_a_repeated_log_schema_is_the_same_version():
    registry = _registry()
    first = registry.register_from_log(TABLE, _raw("a", "b"))
    assert registry.register_from_log(TABLE, _raw("a", "b")) is first


def test_the_primary_key_is_part_of_the_hash():
    columns = tuple(_tracked([]).columns)
    for column in columns:
        column.patype = column.patype or "int64"
    assert schema_hash(columns, ("a",)) != schema_hash(columns, ("b",))
    assert schema_hash(columns, ("a",)) != schema_hash(columns, ())
    assert schema_hash(columns, ("a",)) == schema_hash(columns, ("a",))


def test_a_table_without_a_primary_key_has_none():
    registry = _registry()
    assert registry.register_from_catalog(TABLE, _tracked([])).primary_key == ()
    assert registry.register_from_log("s.other", _raw("a")).primary_key == ()
