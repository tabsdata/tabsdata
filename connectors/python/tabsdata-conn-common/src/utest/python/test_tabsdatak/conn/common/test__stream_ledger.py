#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import FileLedger


def table(*values: int) -> pa.Table:
    return pa.table({"id": list(values)})


def test_a_new_ledger_holds_no_data(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a", "s.b"])
    assert (ledger.has_data, ledger.rows, ledger.bytes) == (False, 0, 0)
    assert ledger.manifest() == [[], []]


def test_a_written_table_lands_as_a_parquet_file_of_its_slot(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a"])
    path = ledger.write("s.a", table(1, 2, 3))
    assert path.parent == tmp_path
    assert path.name.startswith("s.a_")
    assert path.suffix == ".parquet"
    assert pq.read_table(path).column("id").to_pylist() == [1, 2, 3]


def test_a_slot_name_is_sanitized_into_the_file_name(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["db/s a"])
    assert ledger.write("db/s a", table(1)).name.startswith("db-s-a_")


def test_rows_and_bytes_count_every_written_file(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a", "s.b"])
    first = ledger.write("s.a", table(1, 2))
    second = ledger.write("s.b", table(3))
    assert ledger.has_data
    assert ledger.rows == 3
    assert ledger.bytes == first.stat().st_size + second.stat().st_size


def test_the_manifest_is_positional_by_slot_with_empty_slots(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a", "s.b", "s.c"])
    first = ledger.write("s.c", table(1))
    second = ledger.write("s.a", table(2))
    third = ledger.write("s.c", table(3))
    assert ledger.manifest() == [[second], [], [first, third]]


def test_the_manifest_is_a_copy(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a"])
    ledger.write("s.a", table(1))
    ledger.manifest()[0].clear()
    assert len(ledger.manifest()[0]) == 1


def test_clearing_forgets_the_files_and_the_counts(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a", "s.b"])
    ledger.write("s.a", table(1, 2))
    ledger.clear()
    assert ledger.manifest() == [[], []]
    assert (ledger.has_data, ledger.rows, ledger.bytes) == (False, 0, 0)


def test_file_names_sort_in_the_order_they_were_written(tmp_path: Path):
    ledger = FileLedger(landing=tmp_path, slots=["s.a"])
    paths = [ledger.write("s.a", table(value)) for value in range(5)]
    assert sorted(paths, key=lambda path: path.name) == paths
