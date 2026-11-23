#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._handlers.columns import (
    ColumnPolicy,
    ColumnPolicyError,
    Zone,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_META_SYSTEM_COLUMN_PREFIX,
    TD_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FRAMEWORK_PREFIXES = (TD_SYSTEM_COLUMN_PREFIX, TD_META_SYSTEM_COLUMN_PREFIX)


def frame() -> TableFrame:
    return TableFrame._build(pl.DataFrame({"x": [1, 2]}).lazy(), CreationMode.MUT)


def sunk(policy: ColumnPolicy, value: TableFrame, path: Path) -> TableFile:
    target = TableFile.local(path)
    policy.sink(value, target).collect()
    return target


def test_sink_is_lazy_until_collected(tmp_path):
    path = tmp_path / "nested" / "t.t"
    plan = ColumnPolicy(Zone.HOST).sink(frame(), TableFile.local(path))
    assert not path.exists()
    pl.collect_all([plan], engine="streaming")
    assert pl.read_parquet(path).select("x").to_series().to_list() == [1, 2]


def test_sink_to_a_stored_location_creates_its_folders(tmp_path):
    path = tmp_path / "data" / "v1" / "0.t"
    table = TableFile.stored(Location(uri=path.as_uri(), env_prefix="ROOT"))
    ColumnPolicy(Zone.HOST).sink(frame(), table).collect()
    assert path.is_file()


@pytest.mark.parametrize("zone", [Zone.HOST, Zone.LAND])
def test_stored_and_landed_frames_keep_framework_columns(tmp_path, zone):
    sunk(ColumnPolicy(zone), frame(), tmp_path / "frame.t")
    columns = pl.read_parquet_schema(tmp_path / "frame.t")
    assert any(column.startswith(TD_SYSTEM_COLUMN_PREFIX) for column in columns)


def test_egress_keeps_only_user_columns(tmp_path):
    sunk(ColumnPolicy(Zone.EGRESS), frame(), tmp_path / "frame.parquet")
    columns = pl.read_parquet_schema(tmp_path / "frame.parquet")
    assert list(columns) == ["x"]
    assert not any(column.startswith(FRAMEWORK_PREFIXES) for column in columns)


def test_a_landed_frame_scans_for_egress(tmp_path):
    land = ColumnPolicy(Zone.LAND)
    landed = sunk(land, frame(), tmp_path / "frame.t")
    shipped = sunk(
        ColumnPolicy(Zone.EGRESS), land.scan(landed), tmp_path / "frame.parquet"
    )
    assert shipped.read().to_dict(as_series=False) == {"x": [1, 2]}


@pytest.mark.parametrize("zone", [Zone.INGRESS, Zone.HOST, Zone.EGRESS])
def test_a_zone_without_an_entry_rule_reads_nothing(tmp_path, zone):
    with pytest.raises(ColumnPolicyError, match=zone.name):
        ColumnPolicy(zone).scan(TableFile.local(tmp_path / "frame.t"))


def test_ingress_has_no_exit_rule_yet(tmp_path):
    with pytest.raises(ColumnPolicyError, match="INGRESS"):
        ColumnPolicy(Zone.INGRESS).sink(frame(), TableFile.local(tmp_path / "f.t"))
