#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any

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
    ForeignBusError,
    Handoff,
    MissingOutputError,
    OutputCopyError,
    SourceHandoff,
    UnsupportedPluginError,
)
from tabsdatak.server._tdrunner.function._handlers.landing import Origin
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import (
    DestBus,
    DestBusOfArrow,
    DestBusOfDataFrame,
    DestBusOfFile,
    DestBusOfLazyFrame,
    DestContext,
    DestPlugin,
    SrcBus,
    SrcBusOfArrow,
    SrcBusOfDataFrame,
    SrcBusOfFile,
    SrcBusOfLazyFrame,
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


class LegacySource(SrcPlugin):
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


def test_a_legacy_source_plugin_has_no_handoff_yet(tmp_path):
    with pytest.raises(UnsupportedPluginError, match="LegacySource"):
        Handoff.of(LegacySource(), tmp_path)


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


# --- source handoff -----------------------------------------------------------

ORDERS = pl.DataFrame({"id": [1, 2]})
NOTHING: Any = None


def write_all(
    bus: SrcBus,
    writes: tuple[list[Any], ...],
) -> SrcBus:
    for position, items in enumerate(writes):
        for item in items:
            bus[position].write(item)
    return bus


class LazyFrameSource(SrcPlugin[Any, Any, SrcBusOfLazyFrame]):
    def __init__(self, *writes: list[Any]) -> None:
        self.writes = writes

    def read_in(self, ctx, conn, src) -> SrcBusOfLazyFrame:
        return write_all(self.bus, self.writes)


class DataFrameSource(SrcPlugin[Any, Any, SrcBusOfDataFrame]):
    def __init__(self, *writes: list[Any]) -> None:
        self.writes = writes

    def read_in(self, ctx, conn, src) -> SrcBusOfDataFrame:
        return write_all(self.bus, self.writes)


class ArrowSource(SrcPlugin[Any, Any, SrcBusOfArrow]):
    def __init__(self, *writes: list[Any]) -> None:
        self.writes = writes

    def read_in(self, ctx, conn, src) -> SrcBusOfArrow:
        return write_all(self.bus, self.writes)


class FileSource(SrcPlugin[Any, Any, SrcBusOfFile]):
    def __init__(self, *writes: list[Any]) -> None:
        self.writes = writes

    def read_in(self, ctx, conn, src) -> SrcBusOfFile:
        return write_all(self.bus, self.writes)


class ForeignSource(SrcPlugin[Any, Any, SrcBusOfLazyFrame]):
    def read_in(self, ctx, conn, src) -> SrcBusOfLazyFrame:
        return SrcBusOfLazyFrame([])


def orders_as(
    bus: type[SrcBus],
    folder: Path,
) -> Any:
    if bus is SrcBusOfLazyFrame:
        return ORDERS.lazy()
    if bus is SrcBusOfDataFrame:
        return ORDERS
    if bus is SrcBusOfArrow:
        table = ORDERS.to_arrow()
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    file = folder / "orders.parquet"
    ORDERS.write_parquet(file)
    return file


def run_source(
    root: Path,
    plugin: SrcPlugin,
    names: list[str],
) -> tuple[SourceHandoff, SrcBus]:
    chosen = Handoff.of(plugin, root)
    bus = chosen.seed(landing_info(root), names)
    chosen.grow(
        plugin,
        NOTHING,
        NOTHING,
        NOTHING,
        bus,
    )
    return chosen, bus


def landed_values(location) -> dict:
    return pl.read_parquet(TableFile.stored(location).locator().uri).to_dict(
        as_series=False,
    )


def test_a_bus_source_gets_the_bus_it_declares(tmp_path):
    chosen = Handoff.of(LazyFrameSource(), tmp_path)
    assert isinstance(chosen, SourceHandoff)
    assert chosen.bus is SrcBusOfLazyFrame


@pytest.mark.parametrize(
    "source, bus",
    [
        (LazyFrameSource, SrcBusOfLazyFrame),
        (DataFrameSource, SrcBusOfDataFrame),
        (ArrowSource, SrcBusOfArrow),
        (FileSource, SrcBusOfFile),
    ],
)
def test_every_source_bus_lands_what_the_plugin_writes(tmp_path, source, bus):
    plugin = source([orders_as(bus, tmp_path)])
    chosen, _ = run_source(tmp_path, plugin, ["orders"])
    [[landed]] = chosen.reap()
    assert (landed.position, landed.index, landed.rows) == (0, 0, 2)
    assert landed.origin is Origin.DECLARED
    assert landed.data.uri.endswith("/landing/0.0.t")
    assert landed_values(landed.data) == {"id": [1, 2]}


def test_source_channels_are_named_after_the_inputs(tmp_path):
    chosen = Handoff.of(LazyFrameSource(), tmp_path)
    bus = chosen.seed(landing_info(tmp_path), ["orders", "customers"])
    assert [(channel.name, channel.position) for channel in bus] == [
        ("orders", 0),
        ("customers", 1),
    ]


def test_each_write_to_a_channel_lands_a_file_of_its_own(tmp_path):
    strings = pl.LazyFrame({"id": ["7"]})
    plugin = LazyFrameSource(
        [ORDERS.lazy(), strings],
        [],
    )
    chosen = Handoff.of(plugin, tmp_path)
    bus = chosen.seed(landing_info(tmp_path), ["orders", "customers"])
    chosen.grow(
        plugin,
        NOTHING,
        NOTHING,
        NOTHING,
        bus,
    )
    [first, second], none = chosen.reap()
    assert (first.index, second.index, none) == (0, 1, [])
    assert second.data.uri.endswith("/landing/0.1.t")
    assert landed_values(second.data) == {"id": ["7"]}


def test_a_write_lands_its_schema_and_metadata(tmp_path):
    class Untyped(SrcPlugin[Any, Any, SrcBusOfLazyFrame]):
        def read_in(self, ctx, conn, src) -> SrcBusOfLazyFrame:
            self.bus["orders"].write(
                pl.LazyFrame({"id": ["7"]}),
                schema=pl.Schema({"id": pl.Int64}),
                metadata={"@path": "/data/a.csv"},
            )
            return self.bus

    chosen, _ = run_source(tmp_path, Untyped(), ["orders"])
    [[landed]] = chosen.reap()
    assert landed.origin is Origin.INFERRED
    assert landed.metadata == {"@path": "/data/a.csv"}


def test_a_source_must_return_the_bus_it_was_given(tmp_path):
    with pytest.raises(ForeignBusError, match="ForeignSource.read_in returned"):
        run_source(tmp_path, ForeignSource(), ["orders"])


def test_the_bus_is_withdrawn_after_read_in(tmp_path):
    plugin = LazyFrameSource([ORDERS.lazy()])
    run_source(tmp_path, plugin, ["orders"])
    with pytest.raises(RuntimeError, match="has no bus"):
        plugin.bus
