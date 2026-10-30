#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from tabsdatak.server._tdrunner.function._commons.document.request import Info
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    DestinationHandoff,
    Handoff,
    MissingOutputError,
    OutputCopyError,
    UnsupportedPluginError,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import (
    DestBus,
    DestBusOfArrow,
    DestBusOfDataFrame,
    DestBusOfFile,
    DestBusOfLazyFrame,
    DestContext,
    DestPlugin,
    SrcPlugin,
    TableFileSpec,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def landing_info(root: Path) -> Info:
    landing = {"uri": (root / "landing").as_uri(), "env_prefix": "ROOT"}
    return Info.model_validate(info_payload(function_landing=landing))


def land(
    info: Info,
    slot: int,
    values: list[int],
) -> OutputEntry:
    frame = TableFrame._build(pl.DataFrame({"x": values}).lazy(), CreationMode.MUT)
    target = TableFile.stored(info.landing_file(slot))
    ColumnPolicy(Zone.LAND).sink(frame, target).collect()
    return OutputEntry(name=f"out_{slot}", uri=target.uri)


def landed(info: Info) -> OutputManifest:
    return OutputManifest(
        outputs=(
            land(
                info,
                0,
                [1, 2],
            ),
            land(
                info,
                1,
                [3],
            ),
        )
    )


def handoff(
    root: Path,
    bus: type[DestBus],
    *,
    legacy: bool = False,
):
    return DestinationHandoff(
        base_folder=root / "work",
        bus=bus,
        legacy=legacy,
    )


class LegacyDestination(DestPlugin):
    def write_out(
        self,
        ctx: DestContext,
        conn: object,
        dest: object,
        tables: list[TableFileSpec],
    ) -> None:
        self.received = tables


class LazyFrameDestination(DestPlugin):
    def write_out(  # type: ignore[override]
        self,
        ctx: DestContext,
        conn: object,
        dest: object,
        bus: DestBusOfLazyFrame,
    ) -> None:
        self.received = bus


class Source(SrcPlugin):
    def read_in(
        self,
        ctx,
        conn,
        src,
    ) -> list:
        return []


def values(table) -> dict:
    if isinstance(table, pl.LazyFrame):
        table = table.collect()
    elif isinstance(table, pa.RecordBatchReader):
        table = pl.from_arrow(table.read_all())
    elif isinstance(table, Path):
        table = pl.read_parquet(table)
    return table.to_dict(as_series=False)


def test_a_legacy_destination_gets_the_file_bus_as_a_list(tmp_path):
    chosen = Handoff.of(LegacyDestination(), tmp_path)
    assert (chosen.bus, chosen.legacy) == (DestBusOfFile, True)


def test_a_bus_destination_gets_the_bus_it_declares(tmp_path):
    chosen = Handoff.of(LazyFrameDestination(), tmp_path)
    assert (chosen.bus, chosen.legacy) == (DestBusOfLazyFrame, False)


def test_a_source_plugin_has_no_handoff_yet(tmp_path):
    with pytest.raises(UnsupportedPluginError, match="Source"):
        Handoff.of(Source(), tmp_path)


@pytest.mark.parametrize(
    "bus",
    [DestBusOfLazyFrame, DestBusOfDataFrame, DestBusOfArrow, DestBusOfFile],
)
def test_every_bus_hands_over_each_landed_output_with_the_user_columns(
    tmp_path,
    bus,
):
    info = landing_info(tmp_path)
    seeded = handoff(tmp_path, bus).seed(info, landed(info))
    assert isinstance(seeded, bus)
    assert [channel.name for channel in seeded] == ["out_0", "out_1"]
    assert [channel.position for channel in seeded] == [0, 1]
    assert [values(channel.read()) for channel in seeded] == [
        {"x": [1, 2]},
        {"x": [3]},
    ]


def test_the_file_bus_copies_each_output_under_the_work_folder(tmp_path):
    info = landing_info(tmp_path)
    seeded = handoff(tmp_path, DestBusOfFile).seed(info, landed(info))
    files = [channel.read() for channel in seeded]
    assert [file.name for file in files] == ["0.parquet", "1.parquet"]
    assert all(file.parent.parent == tmp_path / "work" for file in files)


def test_frame_buses_copy_nothing(tmp_path):
    info = landing_info(tmp_path)
    handoff(tmp_path, DestBusOfLazyFrame).seed(info, landed(info))
    assert not (tmp_path / "work").exists()


def test_a_legacy_plugin_grows_with_the_list_of_files(tmp_path):
    info = landing_info(tmp_path)
    chosen = handoff(
        tmp_path,
        DestBusOfFile,
        legacy=True,
    )
    seeded = chosen.seed(info, landed(info))
    plugin = LegacyDestination()
    chosen.grow(
        plugin,
        "context",
        "connection",
        "destination",
        seeded,
    )
    assert plugin.received == [channel.read() for channel in seeded]


def test_a_bus_plugin_grows_with_the_bus(tmp_path):
    info = landing_info(tmp_path)
    chosen = handoff(tmp_path, DestBusOfLazyFrame)
    seeded = chosen.seed(info, landed(info))
    plugin = LazyFrameDestination()
    chosen.grow(
        plugin,
        "context",
        "connection",
        "destination",
        seeded,
    )
    assert plugin.received is seeded


def test_seeding_nothing_gives_an_empty_bus(tmp_path):
    info = landing_info(tmp_path)
    seeded = handoff(tmp_path, DestBusOfLazyFrame).seed(
        info,
        OutputManifest(outputs=()),
    )
    assert len(seeded) == 0


def test_an_output_without_data_is_missing(tmp_path):
    info = landing_info(tmp_path)
    manifest = OutputManifest(
        outputs=(
            land(
                info,
                0,
                [1],
            ),
            OutputEntry(name="out_1"),
        )
    )
    with pytest.raises(MissingOutputError) as raised:
        handoff(tmp_path, DestBusOfLazyFrame).seed(info, manifest)
    assert (raised.value.slot, raised.value.name) == (1, "out_1")


def test_an_unreadable_landing_file_is_reported(tmp_path):
    info = landing_info(tmp_path)
    missing = OutputEntry(name="out_0", uri=info.landing_file(0).uri)
    with pytest.raises(OutputCopyError, match="0.t"):
        handoff(tmp_path, DestBusOfFile).seed(info, OutputManifest(outputs=(missing,)))


def test_reap_removes_what_seed_copied(tmp_path):
    info = landing_info(tmp_path)
    chosen = handoff(tmp_path, DestBusOfFile)
    seeded = chosen.seed(info, landed(info))
    chosen.reap()
    assert not seeded[0].read().parent.exists()
    assert (tmp_path / "work").is_dir()


def test_reap_without_seed_does_nothing(tmp_path):
    handoff(tmp_path, DestBusOfFile).reap()
    assert not (tmp_path / "work").exists()
