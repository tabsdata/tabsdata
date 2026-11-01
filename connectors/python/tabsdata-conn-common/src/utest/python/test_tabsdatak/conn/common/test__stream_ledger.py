#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from tabsdatax.itest.fixture.functions.stream import LocalStage

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import StageLedger
from tabsdatak.spi import LazyFrameInput


def table(*values: int) -> pa.Table:
    return pa.table({"id": list(values)})


@pytest.fixture
def stage(tmp_path: Path) -> LocalStage:
    return LocalStage(tmp_path)


def ids(stage: LocalStage, ledger: StageLedger) -> list[list[list[int]]]:
    return [
        [stage.scan(stats).collect()["id"].to_list() for stats in files]
        for files in ledger.manifest()
    ]


def test_a_new_ledger_holds_no_data(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b"])
    assert (ledger.has_data, ledger.has_files, ledger.rows, ledger.bytes) == (
        False,
        False,
        0,
        0,
    )
    assert ledger.manifest() == [[], []]


def test_a_written_table_is_sent_to_its_slot(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b"])
    ledger.write("s.b", table(1, 2, 3))
    assert ids(stage, ledger) == [[], [[1, 2, 3]]]
    assert ledger.has_files


def test_a_lazy_frame_is_staged_as_it_is(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a"])
    ledger.stage("s.a", pl.LazyFrame({"id": [4, 5]}))
    assert ids(stage, ledger) == [[[4, 5]]]


def test_rows_and_bytes_count_what_the_stage_wrote(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b"])
    ledger.write("s.a", table(1, 2))
    ledger.write("s.b", table(3))
    assert ledger.has_data
    assert ledger.rows == 3
    assert ledger.bytes == sum(
        stats.bytes for files in stage.pending for stats in files
    )


def test_the_manifest_is_positional_by_slot_with_empty_slots(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b", "s.c"])
    ledger.write("s.c", table(1))
    ledger.write("s.a", table(2))
    ledger.write("s.c", table(3))
    assert ids(stage, ledger) == [[[2]], [], [[1], [3]]]


def test_the_manifest_is_a_copy(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a"])
    ledger.write("s.a", table(1))
    ledger.manifest()[0].clear()
    assert len(ledger.manifest()[0]) == 1


def test_publishing_stages_what_was_sent_with_the_state_and_forgets_it(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b"])
    ledger.write("s.a", table(1, 2))
    ledger.publish({"offset": 7})
    ((batch),) = stage.published
    assert [len(files) for files in batch.tables] == [1, 0]
    assert batch.state == {"offset": 7}
    assert ledger.manifest() == [[], []]
    assert (ledger.has_data, ledger.rows, ledger.bytes) == (False, 0, 0)


def test_clearing_forgets_the_tables_and_the_counts(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a", "s.b"])
    ledger.write("s.a", table(1, 2))
    ledger.clear()
    assert ledger.manifest() == [[], []]
    assert (ledger.has_data, ledger.rows, ledger.bytes) == (False, 0, 0)


def test_discarding_unstages_what_the_ledger_sent_and_forgets_it(stage, tmp_path):
    ledger = StageLedger(ctx=stage, slots=["s.a"])
    ledger.write("s.a", table(1))
    ledger.discard()
    assert stage.pending == [[]]
    assert list(tmp_path.glob("*.parquet")) == []
    assert ledger.manifest() == [[]]
    assert ledger.has_data is False


def test_discarding_leaves_what_others_sent(stage):
    ledger = StageLedger(ctx=stage, slots=["s.a"])
    other = stage.send([[LazyFrameInput(pl.LazyFrame({"id": [9]}))]])
    ledger.write("s.a", table(1))
    ledger.discard()
    assert stage.pending == other
