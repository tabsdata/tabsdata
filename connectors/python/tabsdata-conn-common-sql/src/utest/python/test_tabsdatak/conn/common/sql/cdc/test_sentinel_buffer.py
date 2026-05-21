"""``BufferSentinel`` flush thresholds + offset accounting."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion, build_pa_schema
from tabsdatak.conn.common.sql.cdc._model import (
    CdcDataChange,
    CdcOffset,
    CdcSchemaChange,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._sentinel.buffer import BufferSentinel
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


class _StubOffset(CdcOffset):
    def __init__(self, pos: int = 0):
        self.pos = pos

    def update(self, other):
        if isinstance(other, _StubOffset):
            self.pos = other.pos

    def write(self):
        return {"pos": self.pos}

    @classmethod
    def read(cls, data):
        return cls(pos=data["pos"])


def _sv():
    cols = (
        TrackedColumn(name="id", satype=sa.types.Integer(), patype=pa.int64()),
    )
    return SchemaVersion(
        version=0,
        raw=(("id", "INT", None),),
        hash="hash00",
        columns=cols,
        pa_schema=build_pa_schema(cols, CdcFormat()),
    )


def _tt(table: str) -> TrackedTable:
    return TrackedTable(
        data_table=TableObject(schema="s", table=table),
        cdc_table=TableObject(schema="s", table=table),
    )


def _change(table: str, pos: int) -> CdcDataChange:
    return CdcDataChange(
        operation="i",
        transaction="tx",
        sequence=pos,
        table=table,
        schema=_sv(),
        new_values={"id": pos},
        old_values=None,
        offset=_StubOffset(pos=pos),
    )


def _bs(disk=None, max_rows=10, max_secs=1000, max_bytes=None) -> BufferSentinel:
    return BufferSentinel(
        disk_sentinel=disk if disk is not None else MagicMock(),
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
        tables={"s.t1": _tt("t1"), "s.t2": _tt("t2")},
        offset=_StubOffset(),
    )


# --- Empty / has_data / offset advance --------------------------------


def test_starts_empty():
    bs = _bs()
    assert bs.is_empty is True
    assert bs.has_data is False


def test_offset_advances_on_each_change():
    bs = _bs()
    bs.process_changes([_change("s.t1", 1), _change("s.t1", 2), _change("s.t1", 3)])
    assert bs.offset.pos == 3


# --- Untracked table is silently dropped ------------------------------


def test_untracked_table_is_skipped():
    bs = _bs()
    bs.process_changes([_change("s.other", 1)])
    assert bs.is_empty is True


# --- Row-count flush --------------------------------------------------


def test_row_count_flush_invokes_disk():
    disk = MagicMock()
    bs = _bs(disk=disk, max_rows=3)
    bs.process_changes([_change("s.t1", i) for i in range(1, 4)])
    assert disk.process_buffers.call_count == 1
    # After flush the buffer is drained.
    assert bs.is_empty is True


def test_row_count_threshold_not_hit_keeps_data():
    disk = MagicMock()
    bs = _bs(disk=disk, max_rows=10)
    bs.process_changes([_change("s.t1", 1), _change("s.t1", 2)])
    assert disk.process_buffers.call_count == 0
    assert bs.has_data is True


# --- Bytes flush ------------------------------------------------------


def test_bytes_threshold_triggers_flush():
    disk = MagicMock()
    bs = _bs(disk=disk, max_rows=100, max_bytes=1)  # 1 byte cap -> flush every row
    bs.process_changes([_change("s.t1", 1)])
    assert disk.process_buffers.call_count == 1


# --- Time-based expire -----------------------------------------------


def test_expire_does_not_flush_when_idle_below_threshold():
    disk = MagicMock()
    bs = _bs(disk=disk, max_rows=1000, max_secs=1000)
    bs.process_changes([_change("s.t1", 1)])
    bs.expire()  # not enough time elapsed
    assert disk.process_buffers.call_count == 0


def test_expire_flushes_when_idle_above_threshold(monkeypatch):
    disk = MagicMock()
    bs = _bs(disk=disk, max_rows=1000, max_secs=1)
    bs.process_changes([_change("s.t1", 1)])
    # Backdate the timer so the time-check fires.
    bs._last_flushed -= 5
    bs.expire()
    assert disk.process_buffers.call_count == 1


# --- Unsupported event types raise -----------------------------------


def test_schema_change_raises_notimplemented():
    bs = _bs()
    with pytest.raises(NotImplementedError):
        bs.process_changes([CdcSchemaChange(table="s.t1")])


# --- Change with no offset is a bug ---------------------------------


def test_change_without_offset_raises_runtime():
    bs = _bs()
    bad = _change("s.t1", 1)
    bad.offset = None
    with pytest.raises(RuntimeError):
        bs.process_changes([bad])
