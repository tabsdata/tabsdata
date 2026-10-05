#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._encoder import StreamEncoder

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import FileLedger

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offset import StreamOffset

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import (
    BufferSentinel,
    ChunkSentinel,
    DiskSentinel,
)
from tabsdatak.conn.common._stream.constants import MANIFEST_FLOOR_SECS
from tabsdatak.conn.common.error import ConnErrorCode, ConnStreamException


class Clock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def table(*values: int) -> pa.Table:
    return pa.table({"id": list(values)})


def ids(ledger: FileLedger) -> list[list[list[int]]]:
    return [
        [pq.read_table(path).column("id").to_pylist() for path in files]
        for files in ledger.manifest()
    ]


@pytest.fixture
def ledger(tmp_path: Path) -> FileLedger:
    return FileLedger(landing=tmp_path, slots=["s.a", "s.b"])


def disk(
    ledger: FileLedger,
    clock: Clock,
    *,
    max_rows: int | None = None,
    max_bytes: int | None = None,
    max_secs: float = math.inf,
    floor_secs: float = 0,
    buffer: BufferSentinel | None = None,
) -> DiskSentinel:
    return DiskSentinel(
        ledger=ledger,
        buffer=buffer,
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
        floor_secs=floor_secs,
        clock=clock,
    )


def test_an_empty_ledger_is_never_due_even_when_forced(ledger: FileLedger):
    sentinel = disk(ledger, Clock(), max_rows=0, max_bytes=0, max_secs=0)
    assert sentinel.due(force=True) is False


def test_the_disk_is_due_when_forced_with_data(ledger: FileLedger):
    sentinel = disk(ledger, Clock())
    ledger.write("s.a", table(1))
    assert sentinel.due() is False
    assert sentinel.due(force=True) is True


def test_the_disk_is_due_on_the_row_threshold(ledger: FileLedger):
    sentinel = disk(ledger, Clock(), max_rows=3)
    ledger.write("s.a", table(1, 2))
    assert sentinel.due() is False
    ledger.write("s.b", table(3))
    assert sentinel.due() is True


def test_the_disk_is_due_on_the_byte_threshold(ledger: FileLedger):
    ledger.write("s.a", table(1))
    sentinel = disk(ledger, Clock(), max_bytes=ledger.bytes + 1)
    assert sentinel.due() is False
    ledger.write("s.a", table(2))
    assert sentinel.due() is True


def test_the_disk_is_due_once_the_time_threshold_elapses(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_secs=60)
    ledger.write("s.a", table(1))
    clock.now += 59.9
    assert sentinel.due() is False
    clock.now += 0.1
    assert sentinel.due() is True


def test_resetting_the_disk_timer_restarts_the_time_threshold(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_secs=60)
    ledger.write("s.a", table(1))
    clock.now += 60
    sentinel.reset_timer()
    assert sentinel.due() is False
    clock.now += 60
    assert sentinel.due() is True


def test_the_disk_is_not_due_after_the_ledger_is_cleared(ledger: FileLedger):
    sentinel = disk(ledger, Clock(), max_rows=1)
    ledger.write("s.a", table(1))
    ledger.clear()
    assert sentinel.due() is False


def test_nothing_is_due_before_the_floor_even_past_a_threshold(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_rows=1, max_secs=600, floor_secs=60)
    ledger.write("s.a", table(1, 2, 3))
    clock.now += 59.9
    assert sentinel.due() is False
    clock.now += 0.1
    assert sentinel.due() is True


def test_a_threshold_hit_before_the_floor_stages_at_the_floor(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_bytes=1, max_secs=600, floor_secs=60)
    ledger.write("s.a", table(1))
    assert sentinel.due() is False
    clock.now += 60
    assert sentinel.due() is True


def test_past_the_floor_the_time_threshold_still_waits(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_rows=100, max_secs=600, floor_secs=60)
    ledger.write("s.a", table(1))
    clock.now += 599
    assert sentinel.due() is False
    clock.now += 1
    assert sentinel.due() is True


def test_a_time_threshold_below_the_floor_lowers_the_floor(ledger: FileLedger):
    clock = Clock()
    sentinel = disk(ledger, clock, max_rows=1, max_secs=5, floor_secs=60)
    ledger.write("s.a", table(1))
    clock.now += 4.9
    assert sentinel.due() is False
    clock.now += 0.1
    assert sentinel.due() is True


def test_forcing_bypasses_the_floor(ledger: FileLedger):
    sentinel = disk(ledger, Clock(), max_secs=600, floor_secs=60)
    ledger.write("s.a", table(1))
    assert sentinel.due() is False
    assert sentinel.due(force=True) is True


def test_the_default_floor_is_the_manifest_floor(ledger: FileLedger):
    clock = Clock()
    sentinel = DiskSentinel(
        ledger=ledger, max_rows=1, max_bytes=None, max_secs=600, clock=clock
    )
    ledger.write("s.a", table(1))
    clock.now += MANIFEST_FLOOR_SECS - 0.1
    assert sentinel.due() is False
    clock.now += 0.1
    assert sentinel.due() is True


def test_the_disk_counts_what_the_buffer_still_holds(ledger: FileLedger):
    clock = Clock()
    held = buffer(ledger, clock, measure_bytes=True)
    sentinel = disk(ledger, clock, buffer=held, max_rows=3)
    held.add_all([Record("s.a", 1, size=4), Record("s.b", 2, size=6)])
    ledger.write("s.a", table(3))
    assert (sentinel.rows, sentinel.has_data) == (3, True)
    assert sentinel.bytes == ledger.bytes + 10
    assert sentinel.due() is True


def test_the_disk_has_data_when_only_the_buffer_holds_it(ledger: FileLedger):
    clock = Clock()
    held = buffer(ledger, clock)
    sentinel = disk(ledger, clock, buffer=held)
    held.add(Record("s.a", 1))
    assert ledger.has_data is False
    assert sentinel.due(force=True) is True


def chunks(ledger: FileLedger, **limits) -> ChunkSentinel:
    return ChunkSentinel(
        ledger=ledger,
        max_rows=limits.get("max_rows"),
        max_bytes=limits.get("max_bytes"),
    )


def test_chunks_merge_into_one_file_until_the_row_threshold(ledger: FileLedger):
    sentinel = chunks(ledger, max_rows=3)
    sentinel.add("s.a", "v1", table(1))
    sentinel.add("s.a", "v1", table(2))
    assert ids(ledger) == [[], []]
    sentinel.add("s.a", "v1", table(3))
    assert ids(ledger) == [[[1, 2, 3]], []]


def test_chunks_merge_into_one_file_until_the_byte_threshold(ledger: FileLedger):
    sentinel = chunks(ledger, max_bytes=table(1).nbytes * 2)
    sentinel.add("s.a", "v1", table(1))
    assert ids(ledger) == [[], []]
    sentinel.add("s.a", "v1", table(2))
    assert ids(ledger) == [[[1, 2]], []]


def test_a_new_key_writes_out_the_chunks_of_the_previous_one(ledger: FileLedger):
    sentinel = chunks(ledger)
    sentinel.add("s.a", "v1", table(1))
    sentinel.add("s.a", "v1", table(2))
    sentinel.add("s.a", "v2", table(3))
    assert ids(ledger) == [[[1, 2]], []]
    sentinel.flush("s.a")
    assert ids(ledger) == [[[1, 2], [3]], []]


def test_slots_merge_their_chunks_apart(ledger: FileLedger):
    sentinel = chunks(ledger, max_rows=2)
    sentinel.add("s.a", "v1", table(1))
    sentinel.add("s.b", "v1", table(2))
    assert ids(ledger) == [[], []]
    sentinel.add("s.b", "v1", table(3))
    assert ids(ledger) == [[], [[2, 3]]]


def test_flushing_one_slot_leaves_the_others(ledger: FileLedger):
    sentinel = chunks(ledger)
    sentinel.add("s.a", "v1", table(1))
    sentinel.add("s.b", "v1", table(2))
    sentinel.flush("s.b")
    assert ids(ledger) == [[], [[2]]]


def test_flushing_every_slot_writes_what_each_holds(ledger: FileLedger):
    sentinel = chunks(ledger)
    sentinel.add("s.a", "v1", table(1))
    sentinel.add("s.b", "v1", table(2))
    sentinel.flush()
    assert ids(ledger) == [[[1]], [[2]]]


def test_flushing_with_nothing_held_writes_nothing(ledger: FileLedger):
    sentinel = chunks(ledger)
    sentinel.flush()
    sentinel.add("s.a", "v1", table(1))
    sentinel.flush("s.a")
    sentinel.flush("s.a")
    assert ids(ledger) == [[[1]], []]


@dataclass
class Record:
    slot: str | None
    value: int
    size: int = 1


class Trail(StreamOffset):
    def __init__(self, values: list[int] | None = None) -> None:
        self.values = values or []

    def update(self, other: Self) -> None:
        self.values.extend(other.values)

    def write(self) -> str:
        return ",".join(str(value) for value in self.values)

    @classmethod
    def read(cls, data: str) -> Self:
        return cls([int(value) for value in data.split(",")])


class RecordEncoder(StreamEncoder[Record, Trail]):
    def __init__(self, split: bool = False) -> None:
        self.sized = 0
        self.split = split

    def slot(self, record: Record) -> str | None:
        return record.slot

    def size(self, record: Record) -> int:
        self.sized += 1
        return record.size

    def offset(self, record: Record) -> Trail:
        return Trail([record.value])

    def encode(self, slot: str, records: list[Record]):
        values = [record.value for record in records]
        if self.split:
            for value in values:
                yield table(value)
        else:
            yield table(*values)


def buffer(
    ledger: FileLedger,
    clock: Clock,
    *,
    encoder: RecordEncoder | None = None,
    offset: Trail | None = None,
    max_rows: int | None = None,
    max_bytes: int | None = None,
    max_secs: float = math.inf,
    measure_bytes: bool = False,
) -> BufferSentinel:
    return BufferSentinel(
        ledger=ledger,
        encoder=encoder or RecordEncoder(),
        slots=["s.a", "s.b"],
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
        offset=Trail() if offset is None else offset,
        measure_bytes=measure_bytes,
        clock=clock,
    )


def test_a_record_without_a_slot_keeps_only_its_offset(ledger: FileLedger):
    sentinel = buffer(ledger, Clock(), max_rows=1)
    sentinel.add(Record(None, 1))
    assert sentinel.has_data is False
    assert sentinel.due() is False
    sentinel.flush()
    assert ids(ledger) == [[], []]
    assert sentinel.offset.values == [1]


def test_pending_offsets_alone_flush_on_time(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=5)
    sentinel.add(Record(None, 1))
    clock.now += 5
    assert sentinel.due() is True
    sentinel.flush()
    assert ids(ledger) == [[], []]
    assert sentinel.offset.values == [1]
    assert sentinel.due() is False


def test_an_empty_buffer_is_never_due(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=5)
    clock.now += 50
    assert sentinel.due() is False


def test_records_stay_in_memory_below_every_threshold(ledger: FileLedger):
    sentinel = buffer(ledger, Clock(), max_rows=3)
    sentinel.add_all([Record("s.a", 1), Record("s.b", 2)])
    assert sentinel.has_data is True
    assert sentinel.due() is False
    assert ids(ledger) == [[], []]


def test_the_row_threshold_writes_every_slot_out(ledger: FileLedger):
    sentinel = buffer(ledger, Clock(), max_rows=3)
    sentinel.add_all([Record("s.a", 1), Record("s.b", 2), Record("s.a", 3)])
    assert sentinel.has_data is False
    assert ids(ledger) == [[[1, 3]], [[2]]]


def test_the_byte_threshold_writes_the_records_out(ledger: FileLedger):
    sentinel = buffer(ledger, Clock(), max_bytes=10)
    sentinel.add(Record("s.a", 1, size=6))
    assert ids(ledger) == [[], []]
    sentinel.add(Record("s.a", 2, size=4))
    assert ids(ledger) == [[[1, 2]], []]


def test_sizes_are_measured_only_under_a_byte_threshold(ledger: FileLedger):
    encoder = RecordEncoder()
    sentinel = buffer(ledger, Clock(), encoder=encoder, max_rows=10)
    sentinel.add(Record("s.a", 1))
    assert encoder.sized == 0
    assert sentinel.bytes == 0


def test_sizes_are_measured_when_asked_for(ledger: FileLedger):
    encoder = RecordEncoder()
    sentinel = buffer(ledger, Clock(), encoder=encoder, measure_bytes=True)
    sentinel.add(Record("s.a", 1, size=7))
    assert (encoder.sized, sentinel.bytes, sentinel.rows) == (1, 7, 1)


def test_the_buffer_is_due_once_the_time_threshold_elapses(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=60)
    sentinel.add(Record("s.a", 1))
    clock.now += 59.9
    assert sentinel.due() is False
    clock.now += 0.1
    assert sentinel.due() is True
    assert ids(ledger) == [[], []]


def test_a_record_arriving_past_the_time_threshold_writes_out(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=60)
    sentinel.add(Record("s.a", 1))
    clock.now += 60
    sentinel.add(Record("s.a", 2))
    assert ids(ledger) == [[[1, 2]], []]


def test_an_empty_buffer_is_never_due(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_rows=0, max_bytes=0, max_secs=0)
    clock.now += 1_000
    assert sentinel.due() is False


def test_resetting_the_buffer_timer_restarts_the_time_threshold(
    ledger: FileLedger,
):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=60)
    sentinel.add(Record("s.a", 1))
    clock.now += 60
    sentinel.reset_timer()
    assert sentinel.due() is False


def test_a_flush_restarts_the_time_threshold(ledger: FileLedger):
    clock = Clock()
    sentinel = buffer(ledger, clock, max_secs=60)
    sentinel.add(Record("s.a", 1))
    clock.now += 30
    sentinel.flush()
    sentinel.add(Record("s.a", 2))
    clock.now += 59
    assert sentinel.due() is False


def test_every_table_an_encoding_yields_lands_as_its_own_file(ledger: FileLedger):
    sentinel = buffer(ledger, Clock(), encoder=RecordEncoder(split=True))
    sentinel.add_all([Record("s.a", 1), Record("s.a", 2)])
    sentinel.flush()
    assert ids(ledger) == [[[1], [2]], []]


def test_flushing_an_empty_buffer_writes_nothing(ledger: FileLedger):
    sentinel = buffer(ledger, Clock())
    sentinel.flush()
    assert ids(ledger) == [[], []]


def test_the_offset_moves_only_when_its_records_reach_disk(ledger: FileLedger):
    offset = Trail()
    sentinel = buffer(ledger, Clock(), offset=offset)
    sentinel.add(Record("s.a", 1))
    assert offset.values == []
    sentinel.flush()
    assert offset.values == [1]
    assert sentinel.offset is offset


def test_offsets_are_applied_in_the_order_their_records_arrived(ledger: FileLedger):
    offset = Trail()
    sentinel = buffer(ledger, Clock(), offset=offset)
    sentinel.add_all([Record("s.b", 1), Record("s.a", 2), Record("s.b", 3)])
    sentinel.flush()
    assert offset.values == [1, 2, 3]


def test_a_record_without_a_slot_still_moves_the_offset(ledger: FileLedger):
    offset = Trail()
    sentinel = buffer(ledger, Clock(), offset=offset)
    sentinel.add(Record(None, 1))
    sentinel.flush()
    assert offset.values == [1]


def test_a_buffer_without_a_base_offset_rejects_its_first_record(ledger: FileLedger):
    sentinel = BufferSentinel(
        ledger=ledger,
        encoder=RecordEncoder(),
        slots=["s.a"],
        max_rows=None,
        max_bytes=None,
        max_secs=math.inf,
        offset=None,
        clock=Clock(),
    )
    with pytest.raises(ConnStreamException) as raised:
        sentinel.add(Record("s.a", 1))
    assert raised.value.api_error is ConnErrorCode.CONN_33
