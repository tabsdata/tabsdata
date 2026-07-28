#
# Copyright 2026 Tabsdata Inc.
#

"""S3 destination-plugin integration tests using fake context"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.s3 import S3System, s3_system
from tabsdatax.itest.fixture.functions.files import (
    PluginDestCtx,
    PluginSrcCtx,
    download_or_skip,
)
from testi_tabsdatak.conn.s3.backend import dest_conn, run_prefix, src_conn, store

from tabsdatak.conn.common.file.typing_ import DestFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.s3 import S3Dest, S3Src, _plugin as plugin

pytestmark = [pytest.mark.s3, pytest.mark.s3_specific]

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _write_out(system: S3System, prefix: str, dest: S3Dest, tmp_path: Path) -> None:
    """Write ``ROWS`` as the parquet the plugin is handed, then let it land the
    object ``dest`` names."""
    local = tmp_path / f"in-{uuid.uuid4().hex}.parquet"
    pl.DataFrame(ROWS).write_parquet(local, use_pyarrow=True)
    work = tmp_path / f"work-dest-{uuid.uuid4().hex}"
    work.mkdir()
    plugin.S3DestPlugin().write_out(
        PluginDestCtx(work), dest_conn(system, prefix), dest, [local]
    )


@s3_system("s3")
def test_dest_then_src_round_trip(system_s3: S3System, tmp_path: Path):
    prefix = run_prefix(system_s3.sys_path, "td-integ")
    _write_out(system_s3, prefix, S3Dest(paths=["users.parquet"]), tmp_path)

    work = tmp_path / "work-src"
    work.mkdir()
    out = plugin.S3SrcPlugin().read_in(
        PluginSrcCtx(work), src_conn(system_s3, prefix), S3Src(paths=["users.parquet"])
    )

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@s3_system("s3")
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
    system_s3: S3System,
    tmp_path: Path,
    name: str,
    fmt: DestFileFormat,
    reader: Callable[[Path], pl.DataFrame],
):
    prefix = run_prefix(system_s3.sys_path, "td-integ")
    _write_out(system_s3, prefix, S3Dest(paths=[name], format=fmt), tmp_path)

    # Download the raw object to verify the transporter wrote the requested
    # format (reading via the src plugin would re-transcode to parquet).
    fetched = download_or_skip(
        store(system_s3), f"{prefix}/{name}", tmp_path / f"fetched-{name}"
    )
    df = reader(fetched).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS
