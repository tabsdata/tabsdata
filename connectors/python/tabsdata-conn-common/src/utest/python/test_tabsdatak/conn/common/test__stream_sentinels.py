#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import FileLedger

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import ChunkSentinel, DiskSentinel


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
) -> DiskSentinel:
    return DiskSentinel(
        ledger=ledger,
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
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


def test_resetting_the_timer_restarts_the_time_threshold(ledger: FileLedger):
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
