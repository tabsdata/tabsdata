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
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn, GCSDest, GCSSrc
from tabsdatak.conn.gcs._explorer import GCSConnectionExplorer
from tabsdatak.spi import TableFileInput

# noinspection PyProtectedMember
import tabsdatak.conn.gcs._plugin as plugin

pytestmark = pytest.mark.gcs

HANDLER = TransporterHandler1()


def _gcp_creds() -> GCPJson:
    return GCPJson(json=Secret('{"type":"service_account"}'))


def _src_conn(**overrides) -> GCSSrcConn:
    kw = dict(
        bucket="my-bucket",
        credentials=_gcp_creds(),
    )
    kw.update(overrides)
    return GCSSrcConn(**kw)


def _dest_conn(**overrides) -> GCSDestConn:
    kw = dict(
        bucket="my-bucket",
        credentials=_gcp_creds(),
    )
    kw.update(overrides)
    return GCSDestConn(**kw)


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_emits_transporter_gcs_with_env_credentials():
    loc = plugin.build_location(_src_conn(), "raw/users.parquet", HANDLER)
    assert isinstance(loc, transporter1.TransporterGCS)
    assert loc.url == "gs://my-bucket/raw/users.parquet"
    assert loc.service_account_key == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON,
    )


def test_build_location_strips_leading_slash_on_path_entry():
    loc = plugin.build_location(_src_conn(), "/raw/users.parquet", HANDLER)
    assert loc.url == "gs://my-bucket/raw/users.parquet"


def test_build_location_preserves_explicit_gs_uri():
    loc = plugin.build_location(_src_conn(), "gs://other-bucket/x/y.parquet", HANDLER)
    assert loc.url == "gs://other-bucket/x/y.parquet"


# ---------------------------------------------------------------------------
# _run_env / _credentials
# ---------------------------------------------------------------------------


def test_run_env_resolves_credentials_to_server_side_env_var_names():
    env = plugin.run_env(_src_conn(), HANDLER)
    assert env == {
        transporter1.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON: '{"type":"service_account"}',
    }


def test_credentials_rejects_non_gcpjson(monkeypatch):
    class _OtherCreds:
        pass

    conn = MagicMock(spec=GCSSrcConn)
    conn.credentials = _OtherCreds()
    with pytest.raises(TypeError, match="must be GCPJson"):
        plugin.credentials(conn)


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
    src = GCSSrc(paths=["a/b.parquet"])
    out = plugin.GCSSrcPlugin().read_in(_src_ctx(tmp_path), conn, src)

    assert out == [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
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
    dest = GCSDest(paths=["out/x.parquet"])
    parquet = Path(tmp_path / "in.parquet")
    plugin.GCSDestPlugin().write_out(_dest_ctx(tmp_path), conn, dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_gcs_src_def_wires_plugin_and_explorer():
    assert plugin.GCS_SRC.conn is GCSSrcConn
    assert plugin.GCS_SRC.src is GCSSrc
    assert plugin.GCS_SRC.plugin is plugin.GCSSrcPlugin
    assert plugin.GCS_SRC.explorer is GCSConnectionExplorer
    assert plugin.GCS_SRC.type_ == "gcs-file-in"


def test_gcs_dest_def_wires_plugin_and_explorer():
    assert plugin.GCS_DEST.conn is GCSDestConn
    assert plugin.GCS_DEST.dest is GCSDest
    assert plugin.GCS_DEST.plugin is plugin.GCSDestPlugin
    assert plugin.GCS_DEST.explorer is GCSConnectionExplorer
    assert plugin.GCS_DEST.type_ == "gcs-file-out"


def test_gcs_cardinality_uses_paths_length():
    src = GCSSrc(paths=["a", "b", "c"])
    assert plugin.GCS_SRC.cardinality(src) == 3
    dest = GCSDest(paths=["a", "b"])
    assert plugin.GCS_DEST.cardinality(dest) == 2


def test_reader_conf_carries_the_drop_metadata_src_cfg_key():
    src = GCSSrc(
        paths=["a/b.parquet"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    rc = _file_plugin.ReaderConf.from_src(object(), src)
    assert rc.drop_metadata is True
