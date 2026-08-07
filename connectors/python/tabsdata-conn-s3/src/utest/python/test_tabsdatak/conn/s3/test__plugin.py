#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin

# noinspection PyProtectedMember
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.invoker import transporter1
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.conn.s3._explorer import S3ConnectionExplorer
from tabsdatak.spi import TableFileInput

# noinspection PyProtectedMember
import tabsdatak.conn.s3._plugin as plugin

pytestmark = pytest.mark.s3

HANDLER = TransporterHandler1()


def _aws_creds() -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret("AKIA0000000000000000"),
        secret_access_key=Secret("secret-value"),
    )


def _src_conn(**overrides) -> S3SrcConn:
    kw = dict(
        bucket="my-bucket",
        region="us-east-1",
        credentials=_aws_creds(),
    )
    kw.update(overrides)
    return S3SrcConn(**kw)


def _dest_conn(**overrides) -> S3DestConn:
    kw = dict(
        bucket="my-bucket",
        region="us-east-1",
        credentials=_aws_creds(),
    )
    kw.update(overrides)
    return S3DestConn(**kw)


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_emits_transporter_s3_with_env_credentials():
    loc = plugin.build_location(_src_conn(), "raw/users.parquet", HANDLER)
    assert isinstance(loc, transporter1.TransporterS3)
    assert loc.url == "s3://my-bucket/raw/users.parquet"
    assert loc.access_key == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_AWS_ACCESS_KEY_ID,
    )
    assert loc.secret_key == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_AWS_SECRET_ACCESS_KEY,
    )
    assert loc.region == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_AWS_REGION,
    )


def test_build_location_strips_leading_slash_on_path_entry():
    loc = plugin.build_location(_src_conn(), "/raw/users.parquet", HANDLER)
    assert loc.url == "s3://my-bucket/raw/users.parquet"


def test_build_location_preserves_explicit_s3_uri():
    loc = plugin.build_location(_src_conn(), "s3://other-bucket/x/y.parquet", HANDLER)
    assert loc.url == "s3://other-bucket/x/y.parquet"


# ---------------------------------------------------------------------------
# _run_env
# ---------------------------------------------------------------------------


def test_run_env_resolves_credentials_to_server_side_env_var_names():
    env = plugin.run_env(_src_conn(), HANDLER)
    assert env == {
        transporter1.SERVER_SIDE_AWS_ACCESS_KEY_ID: "AKIA0000000000000000",
        transporter1.SERVER_SIDE_AWS_SECRET_ACCESS_KEY: "secret-value",
        transporter1.SERVER_SIDE_AWS_REGION: "us-east-1",
    }


# ---------------------------------------------------------------------------
# Plugin classes: delegate to _file_plugin orchestrators with the right wiring
# ---------------------------------------------------------------------------


def _src_ctx(work: Path):
    ctx = MagicMock()
    ctx.work_dir = work
    return ctx


def _dest_ctx(work: Path):
    ctx = MagicMock()
    ctx.work_dir = work
    return ctx


def test_src_plugin_passes_build_location_run_env_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, build_location, run_env, errors):
        captured.update(
            ctx=ctx,
            cfg=cfg,
            build_location=build_location,
            run_env=run_env,
            errors=errors,
        )
        # ``import_files`` returns the metadata-carrying inputs itself;
        # the plugin hands them straight back.
        return [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]

    monkeypatch.setattr(_file_plugin, "import_files", fake)

    conn = _src_conn()
    src = S3Src(paths=["a/b.parquet"])
    out = plugin.S3SrcPlugin().read_in(_src_ctx(tmp_path), conn, src)

    assert out == [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    # cfg was assembled from _file_plugin.ReaderConf.from_src(src)
    assert captured["cfg"].paths == ["a/b.parquet"]


def test_dest_plugin_passes_build_location_run_env_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, tables, build_location, run_env, errors):
        captured.update(
            ctx=ctx,
            cfg=cfg,
            tables=tables,
            build_location=build_location,
            run_env=run_env,
            errors=errors,
        )

    monkeypatch.setattr(_file_plugin, "copy_out", fake)

    conn = _dest_conn()
    dest = S3Dest(paths=["out/x.parquet"])
    parquet = Path(tmp_path / "in.parquet")
    plugin.S3DestPlugin().write_out(_dest_ctx(tmp_path), conn, dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_s3_src_def_wires_plugin_and_explorer():
    assert plugin.S3_SRC.conn is S3SrcConn
    assert plugin.S3_SRC.src is S3Src
    assert plugin.S3_SRC.plugin is plugin.S3SrcPlugin
    assert plugin.S3_SRC.explorer is S3ConnectionExplorer
    assert plugin.S3_SRC.type_ == "s3-file-in"


def test_s3_dest_def_wires_plugin_and_explorer():
    assert plugin.S3_DEST.conn is S3DestConn
    assert plugin.S3_DEST.dest is S3Dest
    assert plugin.S3_DEST.plugin is plugin.S3DestPlugin
    assert plugin.S3_DEST.explorer is S3ConnectionExplorer
    assert plugin.S3_DEST.type_ == "s3-file-out"


def test_s3_cardinality_uses_paths_length():
    src = S3Src(paths=["a", "b", "c"])
    assert plugin.S3_SRC.cardinality(src) == 3
    dest = S3Dest(paths=["a", "b"])
    assert plugin.S3_DEST.cardinality(dest) == 2
