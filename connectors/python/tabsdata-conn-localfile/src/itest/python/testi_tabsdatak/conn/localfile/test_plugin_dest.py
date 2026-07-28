#
# Copyright 2026 Tabsdata Inc.
#

"""LocalFile destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx
from testi_tabsdatak.conn.localfile.backend import dest_conn, scratch, src_conn

from tabsdatak.conn.common.file.typing_ import DestFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.localfile import LocalFileDest, LocalFileSrc, _plugin as plugin

pytestmark = pytest.mark.localfile

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _staged_parquet(root: Path, rows: list[dict] = ROWS) -> Path:
    """A parquet input staged outside the connection's base_path -- what the
    destination plugin is handed as the in-flight table."""
    parquet = root / "in.parquet"
    pl.DataFrame(rows).write_parquet(parquet, use_pyarrow=True)
    return parquet


@fs_domain("fs")
def test_dest_then_src_round_trip(domain_fs: FsDomain, request: pytest.FixtureRequest):
    root, base, _ = scratch(domain_fs, request)
    work_dest = root / "work_dest"
    work_dest.mkdir()
    work_src = root / "work_src"
    work_src.mkdir()
    parquet = _staged_parquet(root)

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work_dest),
        dest_conn(base),
        LocalFileDest(paths=["users.parquet"]),
        [parquet],
    )
    assert (base / "users.parquet").exists()

    out = plugin.LocalFileSrcPlugin().read_in(
        PluginSrcCtx(work_src), src_conn(base), LocalFileSrc(paths=["users.parquet"])
    )

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@fs_domain("fs")
def test_dest_format_csv_writes_csv_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    parquet = _staged_parquet(root)

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(base),
        LocalFileDest(paths=["users.csv"], format=DestFileFormat.CSV),
        [parquet],
    )

    out_file = base / "users.csv"
    assert out_file.exists()
    df = pl.read_csv(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@fs_domain("fs")
def test_dest_format_csv_honours_separator_option(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    parquet = _staged_parquet(root, [{"id": 1, "name": "alice"}])

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(base),
        LocalFileDest(
            paths=["users.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"separator": ";"}},
        ),
        [parquet],
    )

    raw = (base / "users.csv").read_text()
    # ``;`` separates the columns; ``,`` would mean the option was ignored.
    assert ";" in raw
    assert "," not in raw


@fs_domain("fs")
def test_dest_format_json_writes_json_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    parquet = _staged_parquet(root)

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(base),
        LocalFileDest(paths=["users.json"], format=DestFileFormat.JSON),
        [parquet],
    )

    out_file = base / "users.json"
    assert out_file.exists()
    # ``DestFileFormat.JSON`` emits NDJSON (one record per line) to match what the
    # src side consumes.
    df = pl.read_ndjson(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@fs_domain("fs")
def test_dest_format_avro_writes_avro_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    parquet = _staged_parquet(root)

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(base),
        LocalFileDest(paths=["users.avro"], format=DestFileFormat.AVRO),
        [parquet],
    )

    out_file = base / "users.avro"
    assert out_file.exists()
    df = pl.read_avro(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


@fs_domain("fs")
def test_dest_format_auto_defaults_to_parquet(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root, base, work = scratch(domain_fs, request)
    parquet = _staged_parquet(root, [{"id": 1, "name": "alice"}])

    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(base),
        LocalFileDest(paths=["users.parquet"], format=DestFileFormat.AUTO),
        [parquet],
    )

    out_file = base / "users.parquet"
    assert out_file.exists()
    df = pl.read_parquet(out_file).select("id", "name")
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]
