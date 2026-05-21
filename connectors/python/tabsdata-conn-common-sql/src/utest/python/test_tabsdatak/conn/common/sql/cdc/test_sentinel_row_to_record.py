"""``row_to_record`` per-row record builder."""

from __future__ import annotations

import json

import pyarrow as pa
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion, build_pa_schema
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange
from tabsdatak.conn.common.sql.cdc._sentinel import row_to_record
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


def _sv() -> SchemaVersion:
    cols = (
        TrackedColumn(name="id", satype=sa.types.Integer(), patype=pa.int64()),
        TrackedColumn(name="name", satype=sa.types.String(), patype=pa.utf8()),
    )
    return SchemaVersion(
        version=0,
        raw=(("id", "INT", None), ("name", "VARCHAR", None)),
        hash="deadbeef00000000",
        columns=cols,
        pa_schema=build_pa_schema(cols, CdcFormat()),
    )


def _change(operation: str = "i", new=None, old=None) -> CdcDataChange:
    return CdcDataChange(
        operation=operation,
        transaction="tx-1",
        sequence=42,
        table="s.t",
        schema=_sv(),
        new_values=new if new is not None else {"id": 1, "name": "alice"},
        old_values=old,
    )


def test_meta_columns_always_present():
    rec = row_to_record(_change(), CdcFormat())
    assert rec[CDC_META_OP] == "i"
    assert rec[CDC_META_TX] == "tx-1"
    assert rec[CDC_META_SQ] == 42
    assert rec[CDC_META_FMT] == "columns"
    assert rec[CDC_META_FLAT] is True
    assert rec[CDC_META_SCH] == "deadbeef00000000"


def test_flatten_true_inlines_new_values_at_top_level():
    rec = row_to_record(_change(), CdcFormat(flatten_columns=True))
    assert rec["id"] == 1
    assert rec["name"] == "alice"


def test_columns_layout_with_flatten_false():
    fmt = CdcFormat(columns_format="columns", flatten_columns=False)
    rec = row_to_record(
        _change(new={"id": 1, "name": "alice"}, old={"id": 0, "name": "old"}),
        fmt,
    )
    assert rec[f"{CDC_DATA_COL_NEW_PREFIX}.id"] == 1
    assert rec[f"{CDC_DATA_COL_NEW_PREFIX}.name"] == "alice"
    assert rec[f"{CDC_DATA_COL_OLD_PREFIX}.id"] == 0
    assert rec[f"{CDC_DATA_COL_OLD_PREFIX}.name"] == "old"


def test_struct_layout_with_flatten_false():
    fmt = CdcFormat(columns_format="struct", flatten_columns=False)
    rec = row_to_record(
        _change(new={"id": 1}, old={"id": 0}),
        fmt,
    )
    assert rec[CDC_DATA_ROW_NEW] == {"id": 1, "name": None}
    assert rec[CDC_DATA_ROW_OLD] == {"id": 0, "name": None}


def test_map_layout_encodes_json():
    fmt = CdcFormat(columns_format="map", flatten_columns=False)
    rec = row_to_record(
        _change(new={"id": 1, "name": "alice"}, old=None),
        fmt,
    )
    assert json.loads(rec[CDC_DATA_MAP_NEW]) == {"id": 1, "name": "alice"}
    # old_values=None is rendered as {col: None for each column}.
    assert json.loads(rec[CDC_DATA_MAP_OLD]) == {"id": None, "name": None}


def test_old_values_none_handled_in_columns_layout():
    # ``CdcDataChange.old_values`` is allowed to be None (insert).
    rec = row_to_record(
        _change(new={"id": 1, "name": "alice"}, old=None),
        CdcFormat(),  # flatten_columns=True default
    )
    # Old payload follows columns_format="columns" (default).
    assert rec[f"{CDC_DATA_COL_OLD_PREFIX}.id"] is None
    assert rec[f"{CDC_DATA_COL_OLD_PREFIX}.name"] is None
