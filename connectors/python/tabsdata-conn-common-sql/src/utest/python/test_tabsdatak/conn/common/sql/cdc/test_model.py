"""Model-level dataclasses + offset abstract contract."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion, build_pa_schema
from tabsdatak.conn.common.sql.cdc._model import (
    CdcArchitecture,
    CdcDataChange,
    CdcOffset,
    CdcSchemaChange,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


# --- TableObject -----------------------------------------------------


def test_table_object_qualified():
    obj = TableObject(schema="public", table="orders")
    assert obj.qualified == "public.orders"


def test_table_object_is_frozen_and_hashable():
    obj = TableObject(schema="s", table="t")
    # Frozen dataclass: setattr raises
    with pytest.raises(Exception):
        obj.schema = "other"  # type: ignore[misc]
    # And hashable -- can be a dict key.
    {obj: 1}


# --- TrackedTable ----------------------------------------------------


def test_tracked_table_qualified_delegates_to_data_table():
    data = TableObject(schema="s", table="t")
    cdc = TableObject(schema="s", table="t__cdc")
    tt = TrackedTable(data_table=data, cdc_table=cdc)
    assert tt.qualified == "s.t"


def test_tracked_table_post_init_fills_patype():
    columns = [
        TrackedColumn(name="id", satype=sa.types.Integer()),
        TrackedColumn(name="name", satype=sa.types.String()),
    ]
    tt = TrackedTable(
        data_table=TableObject(schema="s", table="t"),
        cdc_table=TableObject(schema="s", table="t"),
        columns=columns,
    )
    # `build_pa_dtypes` fired in __post_init__ and filled patype
    # from the sqlalchemy_to_arrow map.
    assert tt.columns[0].patype is not None
    assert tt.columns[1].patype is not None


# --- CdcOffset abstract ---------------------------------------------


def test_cdc_offset_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        CdcOffset()  # type: ignore[abstract]


class _StubOffset(CdcOffset):
    def __init__(self, pos: int):
        self.pos = pos

    def update(self, other):
        if not isinstance(other, _StubOffset):
            raise TypeError(other)
        self.pos = other.pos

    def write(self) -> dict:
        return {"pos": self.pos}

    @classmethod
    def read(cls, data: dict):
        return cls(pos=data["pos"])


def test_cdc_offset_subclass_round_trip():
    a = _StubOffset(pos=1)
    a.update(_StubOffset(pos=42))
    assert a.pos == 42
    wire = a.write()
    back = _StubOffset.read(wire)
    assert back.pos == 42


# --- CdcDataChange size accounting ---------------------------------


def _sv() -> SchemaVersion:
    import pyarrow as pa
    columns = (
        TrackedColumn(name="id", satype=sa.types.Integer(), patype=pa.int64()),
    )
    return SchemaVersion(
        version=0,
        raw=(("id", "INT", None),),
        hash="deadbeef00000000",
        columns=columns,
        pa_schema=build_pa_schema(columns, CdcFormat()),
    )


_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_cdc_data_change_sizeof_is_positive():
    change = CdcDataChange(
        operation="i",
        transaction="tx-1",
        sequence=1,
        timestamp=_TS,
        table="s.t",
        schema=_sv(),
        new_values={"id": 1, "name": "alice"},
        old_values=None,
    )
    assert change.__sizeof__() > 0


def test_cdc_data_change_with_old_values_is_larger_than_without():
    sv = _sv()
    a = CdcDataChange(
        operation="u",
        transaction="tx-1",
        sequence=1,
        timestamp=_TS,
        table="s.t",
        schema=sv,
        new_values={"id": 1},
        old_values=None,
    )
    b = CdcDataChange(
        operation="u",
        transaction="tx-1",
        sequence=1,
        timestamp=_TS,
        table="s.t",
        schema=sv,
        new_values={"id": 1},
        old_values={"id": 0},
    )
    assert b.__sizeof__() > a.__sizeof__()


# --- CdcSchemaChange / CdcArchitecture -----------------------------


def test_cdc_schema_change_table_can_be_none():
    assert CdcSchemaChange(table=None).table is None
    assert CdcSchemaChange(table="s.t").table == "s.t"


def test_cdc_architecture_is_a_str_enum_with_no_members():
    # Base marker; per-dialect subclasses populate members.
    assert isinstance(CdcArchitecture, type)
    assert list(CdcArchitecture) == []
