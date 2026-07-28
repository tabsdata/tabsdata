#
# Copyright 2026 Tabsdata Inc.
#

"""GCS destination-plugin integration tests using fake context"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.gcs import GcsSystem, gcs_system
from tabsdatax.itest.fixture.functions.files import (
    PluginDestCtx,
    PluginSrcCtx,
    download_or_skip,
)
from testi_tabsdatak.conn.gcs.backend import dest_conn, run_prefix, src_conn, store

from tabsdatak.conn.common.file.typing_ import DestFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.gcs import GCSDest, GCSSrc, _plugin as plugin

pytestmark = pytest.mark.gcs

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _write_out(system: GcsSystem, prefix: str, dest: GCSDest, tmp_path: Path) -> None:
    """Write ``ROWS`` as the parquet the plugin is handed, then let it land the
    object ``dest`` names."""
    local = tmp_path / f"in-{uuid.uuid4().hex}.parquet"
    pl.DataFrame(ROWS).write_parquet(local, use_pyarrow=True)
    work = tmp_path / f"work-dest-{uuid.uuid4().hex}"
    work.mkdir()
    plugin.GCSDestPlugin().write_out(
        PluginDestCtx(work), dest_conn(system, prefix), dest, [local]
    )


@gcs_system("gcs")
def test_dest_then_src_round_trip(system_gcs: GcsSystem, tmp_path: Path):
    prefix = run_prefix(system_gcs.sys_path, "td-integ")
    _write_out(system_gcs, prefix, GCSDest(paths=["users.parquet"]), tmp_path)

    work = tmp_path / "work-src"
    work.mkdir()
    out = plugin.GCSSrcPlugin().read_in(
        PluginSrcCtx(work),
        src_conn(system_gcs, prefix),
        GCSSrc(paths=["users.parquet"]),
    )

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@gcs_system("gcs")
@pytest.mark.parametrize(
    "name, fmt, reader",
    [
        ("out.csv", DestFileFormat.CSV, pl.read_csv),
        ("out.json", DestFileFormat.JSON, pl.read_ndjson),
        ("out.avro", DestFileFormat.AVRO, pl.read_avro),
        ("out.parquet", DestFileFormat.PARQUET, pl.read_parquet),
    ],
)
def test_dest_writes_each_format(
    system_gcs: GcsSystem,
    tmp_path: Path,
    name: str,
    fmt: DestFileFormat,
    reader: Callable[[Path], pl.DataFrame],
):
    prefix = run_prefix(system_gcs.sys_path, "td-integ")
    _write_out(system_gcs, prefix, GCSDest(paths=[name], format=fmt), tmp_path)

    # Download the raw object to verify the transporter wrote the requested
    # format (reading via the src plugin would re-transcode to parquet).
    fetched = download_or_skip(
        store(system_gcs), f"{prefix}/{name}", tmp_path / f"fetched-{name}"
    )
    df = reader(fetched).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS
