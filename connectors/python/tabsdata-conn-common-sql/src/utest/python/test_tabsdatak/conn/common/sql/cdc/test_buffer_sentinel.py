#
#  Copyright 2026 Tabs Data Inc.
#

"""Unit tests for
:class:`~tabsdatak.conn.common.sql.cdc._sentinel.buffer.BufferSentinel`
covering the offset invariants (CDC_10 / CDC_11), the unsupported-change
guard, the untracked-table skip, and the row-threshold flush. The disk
sentinel is faked so no parquet is written."""

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


class FakeDisk:
    def __init__(self):
        self.flushes = 0

    def process_buffers(self, buffers) -> None:
        self.flushes += 1


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


def _sentinel(*, offset, max_rows=1_000, disk=None) -> BufferSentinel:
    return BufferSentinel(
        disk_sentinel=disk or FakeDisk(),
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
    disk = FakeDisk()
    sentinel = _sentinel(offset=FakeOffset(), max_rows=1, disk=disk)
    sentinel.process_data_change(_change(offset=FakeOffset()))
    assert disk.flushes == 1
    assert sentinel.is_empty is True
