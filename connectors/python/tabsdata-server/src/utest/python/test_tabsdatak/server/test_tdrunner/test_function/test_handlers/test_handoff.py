#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._commons.document.request import Info
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    FileDestinationHandoff,
    Handoff,
    MissingOutputError,
    OutputCopyError,
    UnsupportedPluginError,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import DestPlugin, SrcPlugin
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def landing_info(root: Path) -> Info:
    landing = {"uri": (root / "landing").as_uri(), "env_prefix": "ROOT"}
    return Info.model_validate(info_payload(function_landing=landing))


def land(info: Info, slot: int, values: list[int]) -> OutputEntry:
    frame = TableFrame._build(pl.DataFrame({"x": values}).lazy(), CreationMode.MUT)
    target = TableFile.stored(info.landing_file(slot))
    ColumnPolicy(Zone.LAND).sink(frame, target).collect()
    return OutputEntry(name=f"out_{slot}", uri=target.uri)


def handoff(root: Path) -> FileDestinationHandoff:
    return FileDestinationHandoff(base_folder=root / "work")


class Destination(DestPlugin):
    def write_out(self, ctx, conn, dest, tables) -> None:
        pass


class Source(SrcPlugin):
    def read_in(self, ctx, conn, src) -> list:
        return []


def test_a_destination_plugin_gets_a_file_destination_handoff(tmp_path):
    assert isinstance(Handoff.of(Destination(), tmp_path), FileDestinationHandoff)


def test_a_source_plugin_has_no_handoff_yet(tmp_path):
    with pytest.raises(UnsupportedPluginError, match="Source"):
        Handoff.of(Source(), tmp_path)


def test_file_handoff_seeds_each_landed_output_as_a_public_local_parquet(tmp_path):
    info = landing_info(tmp_path)
    manifest = OutputManifest(outputs=(land(info, 0, [1, 2]), land(info, 1, [3])))
    tables = handoff(tmp_path).seed(info, manifest)
    assert [table.name for table in tables] == ["0.parquet", "1.parquet"]
    assert all(table.parent.parent == tmp_path / "work" for table in tables)
    assert [pl.read_parquet(table).to_dict(as_series=False) for table in tables] == [
        {"x": [1, 2]},
        {"x": [3]},
    ]


def test_file_handoff_seeds_nothing_for_no_outputs(tmp_path):
    info = landing_info(tmp_path)
    assert handoff(tmp_path).seed(info, OutputManifest(outputs=())) == []


def test_file_handoff_rejects_an_output_without_data(tmp_path):
    info = landing_info(tmp_path)
    manifest = OutputManifest(outputs=(land(info, 0, [1]), OutputEntry(name="out_1")))
    with pytest.raises(MissingOutputError) as raised:
        handoff(tmp_path).seed(info, manifest)
    assert (raised.value.slot, raised.value.name) == (1, "out_1")


def test_file_handoff_reports_an_unreadable_landing_file(tmp_path):
    info = landing_info(tmp_path)
    missing = OutputEntry(name="out_0", uri=info.landing_file(0).uri)
    with pytest.raises(OutputCopyError, match="0.t"):
        handoff(tmp_path).seed(info, OutputManifest(outputs=(missing,)))


def test_file_handoff_grows_by_writing_the_tables_out(tmp_path):
    calls = []

    class Plugin:
        def write_out(self, context, connection, destination, tables):
            calls.append((context, connection, destination, tables))

    tables = [tmp_path / "0.parquet"]
    handoff(tmp_path).grow(Plugin(), "context", "connection", "dest", tables)
    assert calls == [("context", "connection", "dest", tables)]


def test_reap_removes_what_seed_wrote(tmp_path):
    info = landing_info(tmp_path)
    files = handoff(tmp_path)
    tables = files.seed(info, OutputManifest(outputs=(land(info, 0, [1]),)))
    files.reap()
    assert not tables[0].parent.exists()
    assert (tmp_path / "work").is_dir()


def test_reap_without_seed_does_nothing(tmp_path):
    handoff(tmp_path).reap()
    assert not (tmp_path / "work").exists()
