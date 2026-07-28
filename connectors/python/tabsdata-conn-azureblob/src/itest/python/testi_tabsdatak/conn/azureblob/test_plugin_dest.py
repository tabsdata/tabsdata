#
# Copyright 2026 Tabsdata Inc.
#

"""AzureBlob destination-plugin integration tests using fake context"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobSystem, azureblob_system
from tabsdatax.itest.fixture.functions.files import (
    PluginDestCtx,
    PluginSrcCtx,
    download_or_skip,
)
from testi_tabsdatak.conn.azureblob.backend import (
    dest_conn,
    run_prefix,
    src_conn,
    store,
)

# noinspection PyProtectedMember
from tabsdatak.conn.azureblob import AzureBlobDest, AzureBlobSrc, _plugin as plugin
from tabsdatak.conn.common.file.typing_ import DestFileFormat

pytestmark = pytest.mark.azureblob

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _write_out(
    system: AzureblobSystem, prefix: str, dest: AzureBlobDest, tmp_path: Path
) -> None:
    """Write ``ROWS`` as the parquet the plugin is handed, then let it land the
    blob ``dest`` names."""
    local = tmp_path / f"in-{uuid.uuid4().hex}.parquet"
    pl.DataFrame(ROWS).write_parquet(local, use_pyarrow=True)
    work = tmp_path / f"work-dest-{uuid.uuid4().hex}"
    work.mkdir()
    plugin.AzureBlobDestPlugin().write_out(
        PluginDestCtx(work), dest_conn(system, prefix), dest, [local]
    )


@azureblob_system("azureblob")
def test_dest_then_src_round_trip(system_azureblob: AzureblobSystem, tmp_path: Path):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _write_out(
        system_azureblob, prefix, AzureBlobDest(paths=["users.parquet"]), tmp_path
    )

    work = tmp_path / "work-src"
    work.mkdir()
    out = plugin.AzureBlobSrcPlugin().read_in(
        PluginSrcCtx(work),
        src_conn(system_azureblob, prefix),
        AzureBlobSrc(paths=["users.parquet"]),
    )

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@azureblob_system("azureblob")
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
    system_azureblob: AzureblobSystem,
    tmp_path: Path,
    name: str,
    fmt: DestFileFormat,
    reader: Callable[[Path], pl.DataFrame],
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _write_out(
        system_azureblob, prefix, AzureBlobDest(paths=[name], format=fmt), tmp_path
    )

    # Download the raw blob to verify the transporter wrote the requested format
    # (reading via the src plugin would re-transcode to parquet).
    fetched = download_or_skip(
        store(system_azureblob), f"{prefix}/{name}", tmp_path / f"fetched-{name}"
    )
    df = reader(fetched).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS
