#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._commons.document.state.function import (
    FunctionState,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.outputs import (
    Outputs,
    SinkError,
    StatsError,
)
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    State,
    StateOutput,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

STATE_TABLE = "td.fn_state__F-1"

HOST = Outputs(ColumnPolicy(Zone.HOST))


def fake_table_frame() -> TableFrame:
    return TableFrame._build(pl.DataFrame({"x": [1]}).lazy(), CreationMode.MUT)


def stored_path(root: Path, table: str) -> Path:
    return root / "storage" / f"{table}.t"


def stored_targets(tmp_path: Path, *tables: str) -> list[TableFile]:
    return [TableFile.local(stored_path(tmp_path, table)) for table in tables]


def stored_state(root: Path) -> dict:
    return State.load(TableFile.local(stored_path(root, STATE_TABLE))).values


def state_output(tmp_path: Path, values: dict) -> StateOutput:
    source = tmp_path / "state" / STATE_FILE
    source.parent.mkdir(parents=True, exist_ok=True)
    State.save(FunctionState(values), source)
    return StateOutput(
        name=STATE_TABLE,
        source=TableFile.local(source),
        target=TableFile.local(stored_path(tmp_path, STATE_TABLE)),
    )


def counting_collect_all(monkeypatch) -> list[int]:
    collected = []
    collect_all = pl.collect_all

    def counting(plans, **options):
        collected.append(len(plans))
        return collect_all(plans, **options)

    monkeypatch.setattr(
        pl,
        "collect_all",
        counting,
    )
    return collected


def test_sink_writes_each_table_frame(tmp_path):
    targets = stored_targets(
        tmp_path,
        "t1",
        "t2",
    )
    entries = HOST.sink(
        [fake_table_frame(), fake_table_frame()],
        ["t1", "t2"],
        targets,
    ).outputs
    assert [entry.name for entry in entries] == ["t1", "t2"]
    for entry, target in zip(entries, targets):
        assert entry.uri == target.uri
        assert entry.stats == target.stats()
        assert entry.stats.row_count == 1


def test_sink_of_none_records_no_uri_nor_stats(tmp_path):
    entries = HOST.sink(
        [None, fake_table_frame()],
        ["t1", "t2"],
        stored_targets(
            tmp_path,
            "t1",
            "t2",
        ),
    ).outputs
    assert entries[0] == OutputEntry(name="t1")
    assert entries[1].stats is not None
    assert not stored_path(tmp_path, "t1").exists()


def test_sink_of_a_broken_plan_raises_sink(tmp_path, monkeypatch):
    def broken_sink(self, *arguments):
        raise RuntimeError("disk full")

    monkeypatch.setattr(
        TableFrame,
        "_observed",
        broken_sink,
    )
    with pytest.raises(SinkError, match="t1.t") as error:
        HOST.sink(
            [fake_table_frame()],
            ["t1"],
            stored_targets(tmp_path, "t1"),
        )
    assert error.value.table == "t1"
    assert error.value.detail == "disk full"


def test_sink_failing_while_writing_raises_sink_for_every_table(
    tmp_path,
    monkeypatch,
):
    def fail(batch):
        raise RuntimeError("disk full")

    def failing_sink(self, *arguments):
        return pl.LazyFrame({"x": [1]}).map_batches(fail)

    monkeypatch.setattr(
        TableFrame,
        "_observed",
        failing_sink,
    )
    with pytest.raises(SinkError, match="t1, t2") as error:
        HOST.sink(
            [fake_table_frame(), fake_table_frame()],
            ["t1", "t2"],
            stored_targets(
                tmp_path,
                "t1",
                "t2",
            ),
        )
    assert error.value.table == "t1, t2"


def test_sink_with_unreadable_stats_raises_stats(tmp_path, monkeypatch):
    def unreadable(self):
        raise OSError("gone")

    monkeypatch.setattr(
        TableFile,
        "stats",
        unreadable,
    )
    with pytest.raises(StatsError, match="stats of 't1'") as error:
        HOST.sink(
            [fake_table_frame()],
            ["t1"],
            stored_targets(tmp_path, "t1"),
        )
    assert error.value.table == "t1"
    assert error.value.detail == "gone"


def test_sink_writes_every_output_in_one_pass(tmp_path, monkeypatch):
    collected = counting_collect_all(monkeypatch)
    HOST.sink(
        [fake_table_frame(), fake_table_frame(), None],
        ["t1", "t2", "t3"],
        stored_targets(
            tmp_path,
            "t1",
            "t2",
            "t3",
        ),
    )
    assert collected == [2]


def test_sink_writes_the_state_in_the_same_pass(tmp_path, monkeypatch):
    collected = counting_collect_all(monkeypatch)
    state = state_output(tmp_path, {"cursor": 250})
    manifest = HOST.sink(
        [fake_table_frame()],
        ["t1"],
        stored_targets(tmp_path, "t1"),
        state=state,
    )
    assert collected == [2]
    assert [entry.name for entry in manifest.outputs] == ["t1"]
    assert manifest.state == OutputEntry(
        name=STATE_TABLE,
        uri=state.target.uri,
        stats=state.target.stats(),
    )
    assert stored_state(tmp_path) == {"cursor": 250}


def test_sink_writes_the_state_without_outputs(tmp_path):
    manifest = HOST.sink(
        [None],
        ["t1"],
        stored_targets(tmp_path, "t1"),
        state=state_output(tmp_path, {}),
    )
    assert manifest.outputs == (OutputEntry(name="t1"),)
    assert manifest.state.stats.column_count == 0
    assert stored_state(tmp_path) == {}


def test_sink_without_state_records_none(tmp_path):
    manifest = HOST.sink(
        [fake_table_frame()],
        ["t1"],
        stored_targets(tmp_path, "t1"),
    )
    assert manifest.state is None


def test_sink_of_an_unwritable_state_raises_sink(tmp_path, monkeypatch):
    def broken_copy(self, target):
        raise RuntimeError("disk full")

    monkeypatch.setattr(
        TableFile,
        "copy",
        broken_copy,
    )
    with pytest.raises(SinkError, match=STATE_TABLE) as error:
        HOST.sink(
            [None],
            ["t1"],
            stored_targets(tmp_path, "t1"),
            state=state_output(tmp_path, {}),
        )
    assert error.value.table == STATE_TABLE


@pytest.mark.parametrize("zone", [Zone.HOST, Zone.LAND])
def test_sink_keeps_system_columns(tmp_path, zone):
    Outputs(ColumnPolicy(zone)).sink(
        [fake_table_frame()],
        ["t1"],
        stored_targets(tmp_path, "t1"),
    )
    columns = pl.read_parquet_schema(stored_path(tmp_path, "t1"))
    assert any(column.startswith(TD_SYSTEM_COLUMN_PREFIX) for column in columns)
