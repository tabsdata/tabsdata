"""``RawSchema`` hashing and ``build_pa_schema`` payload layouts."""

from __future__ import annotations

import pyarrow as pa
import pytest

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import (
    RawSchema,
    build_pa_schema,
    raw_schema_hash,
)
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_SCH,
    CDC_META_SQ,
    CDC_META_TX,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


def _cols() -> list[TrackedColumn]:
    return [
        TrackedColumn(name="id", satype="<int>", patype=pa.int64()),
        TrackedColumn(name="name", satype="<str>", patype=pa.utf8()),
    ]


# --- raw_schema_hash -------------------------------------------------


def test_raw_schema_hash_is_deterministic():
    rs: RawSchema = (("id", "INT", None), ("name", "VARCHAR", None))
    assert raw_schema_hash(rs) == raw_schema_hash(rs)
    assert len(raw_schema_hash(rs)) == 16


def test_raw_schema_hash_differs_on_column_change():
    a: RawSchema = (("id", "INT", None),)
    b: RawSchema = (("id", "BIGINT", None),)
    assert raw_schema_hash(a) != raw_schema_hash(b)


def test_raw_schema_hash_ignores_raw_type_object():
    # Only `(name, dtype_string)` participate in the canonical form;
    # the raw type object is reserved for the type-mapper.
    a: RawSchema = (("id", "INT", object()),)
    b: RawSchema = (("id", "INT", object()),)
    assert raw_schema_hash(a) == raw_schema_hash(b)


# --- build_pa_schema -- meta columns are always first ---------------


def test_meta_columns_lead_in_every_layout():
    for fmt in (
        CdcFormat(columns_format="columns"),
        CdcFormat(columns_format="struct"),
        CdcFormat(columns_format="map"),
    ):
        schema = build_pa_schema(_cols(), fmt)
        first_six = [schema.field(i).name for i in range(6)]
        assert first_six == [
            CDC_META_OP,
            CDC_META_TX,
            CDC_META_SQ,
            CDC_META_FMT,
            CDC_META_FLAT,
            CDC_META_SCH,
        ]


# --- flatten_columns=True: new values inline at top level ------------


def test_flatten_true_inlines_new_values_at_top_level():
    schema = build_pa_schema(_cols(), CdcFormat(flatten_columns=True))
    names = [f.name for f in schema]
    # After the 6 meta columns, the two source columns appear inline.
    assert "id" in names
    assert "name" in names
    # And the *old* layout still follows `columns_format` -- default
    # is "columns" so old fields are prefixed.
    assert f"{CDC_DATA_COL_OLD_PREFIX}.id" in names
    assert f"{CDC_DATA_COL_OLD_PREFIX}.name" in names


# --- flatten_columns=False: each columns_format produces a distinct
#     payload layout ------------------------------------------------


def test_columns_layout_uses_prefixed_columns_per_source_column():
    fmt = CdcFormat(columns_format="columns", flatten_columns=False)
    schema = build_pa_schema(_cols(), fmt)
    names = [f.name for f in schema]
    assert f"{CDC_DATA_COL_NEW_PREFIX}.id" in names
    assert f"{CDC_DATA_COL_NEW_PREFIX}.name" in names
    assert f"{CDC_DATA_COL_OLD_PREFIX}.id" in names
    assert f"{CDC_DATA_COL_OLD_PREFIX}.name" in names


def test_struct_layout_emits_two_struct_columns():
    fmt = CdcFormat(columns_format="struct", flatten_columns=False)
    schema = build_pa_schema(_cols(), fmt)
    names = [f.name for f in schema]
    assert CDC_DATA_ROW_NEW in names
    assert CDC_DATA_ROW_OLD in names
    # Each is a struct over the source columns.
    new_field = schema.field(CDC_DATA_ROW_NEW)
    assert pa.types.is_struct(new_field.type)


def test_map_layout_emits_two_utf8_columns():
    fmt = CdcFormat(columns_format="map", flatten_columns=False)
    schema = build_pa_schema(_cols(), fmt)
    names = [f.name for f in schema]
    assert CDC_DATA_MAP_NEW in names
    assert CDC_DATA_MAP_OLD in names


def test_old_columns_are_nullable():
    schema = build_pa_schema(_cols(), CdcFormat())
    for old_name in (f"{CDC_DATA_COL_OLD_PREFIX}.id", f"{CDC_DATA_COL_OLD_PREFIX}.name"):
        assert schema.field(old_name).nullable is True
