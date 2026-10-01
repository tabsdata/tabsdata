#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for
:class:`~tabsdatak.conn.common.sql.cdc._sentinel.buffer.BufferSentinel`
covering the offset invariants (CDC_10 / CDC_11), the unsupported-change
guard, the untracked-table skip, and the row-threshold flush. The ledger
and the encoder are faked so no parquet is written."""

from __future__ import annotations

from datetime import datetime

import pytest

from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, CdcSchemaChange
from tabsdatak.conn.common.sql.cdc._sentinel.buffer import BufferSentinel
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException


class FakeOffset:
    def __init__(self):
        self.updates = 0

    def update(self, other) -> None:
        self.updates += 1


class FakeLedger:
    def __init__(self):
        self.writes = 0

    def write(self, slot, table) -> None:
        self.writes += 1


class FakeEncoder:
    def encode(self, table, rows):
        yield rows


def _change(table: str = "s.t", *, offset=None) -> CdcDataChange:
    return CdcDataChange(
        operation="I",
        transaction="tx1",
        sequence=1,
        timestamp=datetime(2026, 1, 1),
        table=table,
        schema=None,
        new_values={"id": 1},
        old_values=None,
        offset=offset,
    )


def _sentinel(*, offset, max_rows=1_000, ledger=None) -> BufferSentinel:
    return BufferSentinel(
        ledger=ledger or FakeLedger(),
        encoder=FakeEncoder(),
        max_rows=max_rows,
        max_bytes=None,
        max_secs=10_000,
        tables={"s.t": None},
        offset=offset,
    )


def test_missing_sentinel_offset_raises_cdc_10():
    sentinel = _sentinel(offset=None)
    with pytest.raises(SqlCdcException):
        sentinel.process_data_change(_change(offset=FakeOffset()))


def test_change_without_offset_raises_cdc_11():
    sentinel = _sentinel(offset=FakeOffset())
    with pytest.raises(SqlCdcException):
        sentinel.process_data_change(_change(offset=None))


def test_unsupported_change_type_raises_not_implemented():
    sentinel = _sentinel(offset=FakeOffset())
    with pytest.raises(NotImplementedError):
        sentinel.process_changes([CdcSchemaChange(table="s.t")])


def test_untracked_table_change_is_skipped():
    sentinel = _sentinel(offset=FakeOffset())
    sentinel.process_data_change(_change(table="other.table", offset=FakeOffset()))
    assert sentinel.is_empty is True


def test_row_threshold_triggers_disk_flush():
    ledger = FakeLedger()
    sentinel = _sentinel(offset=FakeOffset(), max_rows=1, ledger=ledger)
    sentinel.process_data_change(_change(offset=FakeOffset()))
    assert ledger.writes == 1
    assert sentinel.is_empty is True


class RecordingOffset:
    def __init__(self):
        self.applied: list[str] = []

    def update(self, other) -> None:
        self.applied.append(other.name)


class NamedOffset:
    def __init__(self, name: str):
        self.name = name


def test_the_offset_moves_only_when_its_changes_reach_disk():
    """A stage carries the files on disk with this offset: were it past a
    change still in memory, the next run would resume after a change that
    was never staged."""
    offset = RecordingOffset()
    sentinel = _sentinel(offset=offset)
    sentinel.process_data_change(_change(offset=NamedOffset("a")))
    assert offset.applied == []
    sentinel.flush()
    assert offset.applied == ["a"]


def test_offsets_are_applied_in_the_order_their_changes_arrived():
    offset = RecordingOffset()
    sentinel = BufferSentinel(
        ledger=FakeLedger(),
        encoder=FakeEncoder(),
        max_rows=1_000,
        max_bytes=None,
        max_secs=10_000,
        tables={"s.t": None, "s.u": None},
        offset=offset,
    )
    sentinel.process_data_change(_change("s.u", offset=NamedOffset("u1")))
    sentinel.process_data_change(_change("s.t", offset=NamedOffset("t1")))
    sentinel.process_data_change(_change("s.u", offset=NamedOffset("u2")))
    sentinel.flush()
    assert offset.applied == ["u1", "t1", "u2"]
