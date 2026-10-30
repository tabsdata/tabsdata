#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Iterator

import polars as pl
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
    InputCountError,
    MissingOutputError,
    OutputCopyError,
    SourceHandoff,
    UnexpectedTableError,
    UnsupportedPluginError,
    local_file,
)
from tabsdatak.server._tdrunner.function._handlers.landing import Origin
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import (
    DestContext,
    DestPlugin,
    LazyFrameInput,
    SrcPlugin,
    TableFileInput,
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
    *,
    legacy: bool = False,
):
    return DestinationHandoff(
        base_folder=root / "work",
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


class LazyFrameDestination(DestPlugin[Any, Any, list[pl.LazyFrame]]):
    def write_out(
        self,
        ctx: DestContext,
        conn: object,
        dest: object,
        tables: list[pl.LazyFrame],
    ) -> None:
        self.received = tables


def values(table: pl.LazyFrame | Path) -> dict:
    if isinstance(table, Path):
        return pl.read_parquet(table).to_dict(as_series=False)
    return table.collect().to_dict(as_series=False)


def test_a_legacy_destination_gets_the_list_of_files(tmp_path):
    chosen = Handoff.of(LegacyDestination(), tmp_path)
    assert isinstance(chosen, DestinationHandoff)
    assert chosen.legacy


def test_a_frames_destination_gets_the_frames(tmp_path):
    chosen = Handoff.of(LazyFrameDestination(), tmp_path)
    assert isinstance(chosen, DestinationHandoff)
    assert not chosen.legacy


def test_an_unknown_plugin_has_no_handoff(tmp_path):
    with pytest.raises(UnsupportedPluginError, match="object"):
        Handoff.of(object(), tmp_path)


@pytest.mark.parametrize(
    "file",
    [
        "file:///tmp/x.parquet",
        "file:/tmp/x.parquet",
        Path("file:///tmp/x.parquet"),
        "FILE:///tmp/x.parquet",
        "File:/tmp/x.parquet",
    ],
)
def test_a_returned_file_uri_is_read_as_a_local_path(file):
    assert local_file(file) == Path("/tmp/x.parquet")


def test_a_returned_file_uri_is_percent_decoded():
    assert local_file("file:///tmp/with%20space.parquet") == Path(
        "/tmp/with space.parquet",
    )


@pytest.mark.parametrize("file", ["/tmp/x.parquet", "x.parquet"])
def test_a_returned_path_is_read_as_it_is(file):
    assert local_file(file) == Path(file)


@pytest.mark.parametrize("legacy", [False, True])
def test_every_form_hands_over_each_landed_output_with_the_user_columns(
    tmp_path,
    legacy,
):
    info = landing_info(tmp_path)
    slots = handoff(tmp_path, legacy=legacy).seed(info, landed(info))
    assert [(slot.name, slot.position) for slot in slots] == [
        ("out_0", 0),
        ("out_1", 1),
    ]
    assert all(isinstance(slot.table, Path) is legacy for slot in slots)
    assert [values(slot.table) for slot in slots] == [
        {"x": [1, 2]},
        {"x": [3]},
    ]


def test_the_legacy_form_copies_each_output_under_the_work_folder(tmp_path):
    info = landing_info(tmp_path)
    slots = handoff(tmp_path, legacy=True).seed(info, landed(info))
    files = [slot.table for slot in slots]
    assert [file.name for file in files] == ["0.parquet", "1.parquet"]
    assert all(file.parent.parent == tmp_path / "work" for file in files)


def test_frames_copy_nothing(tmp_path):
    info = landing_info(tmp_path)
    handoff(tmp_path).seed(info, landed(info))
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize(
    "plugin, legacy",
    [(LegacyDestination, True), (LazyFrameDestination, False)],
)
def test_a_plugin_grows_with_the_list_of_its_tables(tmp_path, plugin, legacy):
    info = landing_info(tmp_path)
    chosen = handoff(tmp_path, legacy=legacy)
    slots = chosen.seed(info, landed(info))
    destination = plugin()
    chosen.grow(
        destination,
        "context",
        "connection",
        "destination",
        slots,
    )
    assert destination.received == [slot.table for slot in slots]


def test_seeding_nothing_gives_no_slots(tmp_path):
    info = landing_info(tmp_path)
    slots = handoff(tmp_path).seed(
        info,
        OutputManifest(outputs=()),
    )
    assert slots == []


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
        handoff(tmp_path).seed(info, manifest)
    assert (raised.value.slot, raised.value.name) == (1, "out_1")


def test_an_unreadable_landing_file_is_reported(tmp_path):
    info = landing_info(tmp_path)
    missing = OutputEntry(name="out_0", uri=info.landing_file(0).uri)
    with pytest.raises(OutputCopyError, match="0.t"):
        handoff(tmp_path, legacy=True).seed(
            info,
            OutputManifest(outputs=(missing,)),
        )


def test_reap_removes_what_seed_copied(tmp_path):
    info = landing_info(tmp_path)
    chosen = handoff(tmp_path, legacy=True)
    slots = chosen.seed(info, landed(info))
    chosen.reap()
    assert not slots[0].table.parent.exists()
    assert (tmp_path / "work").is_dir()


def test_reap_without_seed_does_nothing(tmp_path):
    handoff(tmp_path, legacy=True).reap()
    assert not (tmp_path / "work").exists()


# --- source handoff -----------------------------------------------------------

ORDERS = pl.DataFrame({"id": [1, 2]})
NOTHING: Any = None


class LazyFrameSource(SrcPlugin):
    def __init__(self, *produced: Any) -> None:
        self.produced = list(produced)

    def read_in(self, ctx, conn, src) -> list[Iterable[LazyFrameInput]]:
        return self.produced


class LegacySource(SrcPlugin):
    def __init__(self, *produced: Any) -> None:
        self.produced = list(produced)

    def read_in(self, ctx, conn, src) -> list[list[TableFileInput]]:
        return self.produced


def run_source(
    root: Path,
    plugin: SrcPlugin,
    names: list[str],
    *,
    variable: bool = False,
) -> SourceHandoff:
    chosen = Handoff.of(plugin, root)
    chosen.seed(landing_info(root), names, variable=variable)
    chosen.grow(
        plugin,
        NOTHING,
        NOTHING,
        NOTHING,
    )
    return chosen


def landed_values(location) -> dict:
    return pl.read_parquet(TableFile.stored(location).locator().uri).to_dict(
        as_series=False,
    )


@pytest.mark.parametrize(
    "plugin, legacy",
    [(LegacySource(), True), (LazyFrameSource(), False)],
)
def test_a_source_gets_the_form_it_returns(tmp_path, plugin, legacy):
    chosen = Handoff.of(plugin, tmp_path)
    assert isinstance(chosen, SourceHandoff)
    assert chosen.legacy is legacy


def test_source_slots_are_named_after_the_inputs(tmp_path):
    chosen = Handoff.of(LazyFrameSource(), tmp_path)
    slots = chosen.seed(landing_info(tmp_path), ["orders", "customers"])
    assert [(slot.name, slot.position) for slot in slots] == [
        ("orders", 0),
        ("customers", 1),
    ]


def test_a_frames_source_lands_what_it_returns(tmp_path):
    chosen = run_source(
        tmp_path,
        LazyFrameSource([LazyFrameInput(ORDERS.lazy())]),
        ["orders"],
    )
    [[landed]] = chosen.reap()
    assert (landed.position, landed.index, landed.rows) == (0, 0, 2)
    assert landed.origin is Origin.DECLARED
    assert landed.data.uri.endswith("/landing/0.0.t")
    assert landed_values(landed.data) == {"id": [1, 2]}


def test_a_legacy_source_lands_its_files_with_their_metadata(tmp_path):
    file = tmp_path / "orders.parquet"
    ORDERS.write_parquet(file)
    plugin = LegacySource([TableFileInput(file=file, metadata={"@page": 1})])
    [[landed]] = run_source(tmp_path, plugin, ["orders"]).reap()
    assert landed.metadata == {"@page": 1}
    assert landed_values(landed.data) == {"id": [1, 2]}


def test_each_table_of_an_input_lands_a_file_of_its_own(tmp_path):
    plugin = LazyFrameSource(
        [LazyFrameInput(ORDERS.lazy()), LazyFrameInput(pl.LazyFrame({"id": ["7"]}))],
        [],
    )
    chosen = run_source(tmp_path, plugin, ["orders", "customers"])
    [first, second], none = chosen.reap()
    assert (first.index, second.index, none) == (0, 1, [])
    assert second.data.uri.endswith("/landing/0.1.t")
    assert landed_values(second.data) == {"id": ["7"]}


def test_a_generator_lands_each_table_as_it_yields_it(tmp_path):
    chosen = Handoff.of(LazyFrameSource(), tmp_path)
    slots = chosen.seed(landing_info(tmp_path), ["orders"])

    def pages() -> Iterator[LazyFrameInput]:
        for page in range(3):
            assert len(slots[0].landed) == page
            yield LazyFrameInput(pl.LazyFrame({"id": [page]}))

    chosen.grow(
        LazyFrameSource(pages()),
        NOTHING,
        NOTHING,
        NOTHING,
    )
    [landed] = chosen.reap()
    assert [landed_values(file.data) for file in landed] == [
        {"id": [0]},
        {"id": [1]},
        {"id": [2]},
    ]


def test_a_lazy_frame_input_lands_its_schema_and_metadata(tmp_path):
    plugin = LazyFrameSource(
        [
            LazyFrameInput(
                pl.LazyFrame({"id": ["7"]}),
                schema=pl.Schema({"id": pl.Int64}),
                metadata={"@path": "/data/a.csv"},
            ),
        ],
    )
    [[landed]] = run_source(tmp_path, plugin, ["orders"]).reap()
    assert landed.origin is Origin.INFERRED
    assert landed.metadata == {"@path": "/data/a.csv"}


@pytest.mark.parametrize(
    "produced, variable",
    [
        ([[], []], False),
        ([], False),
        ([[], []], True),
    ],
)
def test_a_source_returns_one_entry_per_input(tmp_path, produced, variable):
    with pytest.raises(InputCountError) as raised:
        run_source(
            tmp_path,
            LazyFrameSource(*produced),
            ["orders"],
            variable=variable,
        )
    assert (raised.value.returned, raised.value.expected) == (len(produced), 1)


def test_a_variable_source_may_return_fewer_entries(tmp_path):
    chosen = run_source(
        tmp_path,
        LazyFrameSource([LazyFrameInput(ORDERS.lazy())]),
        ["orders", "customers"],
        variable=True,
    )
    assert [len(files) for files in chosen.reap()] == [1, 0]


@pytest.mark.parametrize(
    "plugin, table",
    [
        (LazyFrameSource, ORDERS.lazy()),
        (LazyFrameSource, ORDERS),
        (LegacySource, LazyFrameInput(ORDERS.lazy())),
    ],
)
def test_a_source_returns_only_tables_of_its_form(tmp_path, plugin, table):
    with pytest.raises(UnexpectedTableError, match="for input 0"):
        run_source(tmp_path, plugin([table]), ["orders"])
