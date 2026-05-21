from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.api import Secret
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSConn, GCSDest, GCSSrc
# noinspection PyProtectedMember
import tabsdatak.conn.gcs._plugin as plugin
from tabsdatak.spi import ParquetFile


pytestmark = pytest.mark.gcs


def _gcp_creds() -> GCPJson:
    return GCPJson(json=Secret.lit('{"type":"service_account"}'))


def _conn(**overrides) -> GCSConn:
    kw = dict(
        bucket="my-bucket",
        credentials=_gcp_creds(),
    )
    kw.update(overrides)
    return GCSConn(**kw)


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_emits_transporter_gcs_with_env_credentials():
    loc = plugin.build_location(_conn(), "raw/users.parquet")
    assert isinstance(loc, _transporter.TransporterGCS)
    assert loc.url == "gs://my-bucket/raw/users.parquet"
    assert loc.service_account_key == _transporter.TransporterEnv(
        _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON,
    )


def test_build_location_strips_leading_slash_on_path_entry():
    loc = plugin.build_location(_conn(), "/raw/users.parquet")
    assert loc.url == "gs://my-bucket/raw/users.parquet"


def test_build_location_preserves_explicit_gs_uri():
    loc = plugin.build_location(_conn(), "gs://other-bucket/x/y.parquet")
    assert loc.url == "gs://other-bucket/x/y.parquet"


# ---------------------------------------------------------------------------
# _run_env / _credentials
# ---------------------------------------------------------------------------


def test_run_env_resolves_credentials_to_server_side_env_var_names():
    env = plugin.run_env(_conn())
    assert env == {
        _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON: '{"type":"service_account"}',
    }


def test_credentials_rejects_non_gcpjson(monkeypatch):
    class _OtherCreds:
        pass

    conn = MagicMock(spec=GCSConn)
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
            ctx=ctx, cfg=cfg, build_location=build_location,
            run_env=run_env, errors=errors,
        )
        return [[ParquetFile(tmp_path / "ok.parquet")]]

    monkeypatch.setattr(_file_plugin, "import_files", fake)

    src = GCSSrc(conn=_conn(), paths=["a/b.parquet"])
    out = plugin.GCSSrcPlugin().read_in(_src_ctx(tmp_path), src)

    assert out == [[ParquetFile(tmp_path / "ok.parquet")]]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["a/b.parquet"]


def test_dest_plugin_passes_build_location_run_env_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, tables, build_location, run_env, errors):
        captured.update(
            ctx=ctx, cfg=cfg, tables=tables, build_location=build_location,
            run_env=run_env, errors=errors,
        )

    monkeypatch.setattr(_file_plugin, "copy_out", fake)

    dest = GCSDest(conn=_conn(), paths=["out/x.parquet"])
    parquet = ParquetFile(tmp_path / "in.parquet")
    plugin.GCSDestPlugin().write_out(_dest_ctx(tmp_path), dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.GCSExplorer()
    assert exp.metadata(_conn()) is None
    assert exp.data(_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_gcs_src_def_wires_plugin_and_explorer():
    assert plugin.GCS_SRC.conn is GCSConn
    assert plugin.GCS_SRC.src is GCSSrc
    assert plugin.GCS_SRC.plugin is plugin.GCSSrcPlugin
    assert plugin.GCS_SRC.explorer is plugin.GCSExplorer
    assert plugin.GCS_SRC.src_type == "gcs"


def test_gcs_dest_def_wires_plugin_and_explorer():
    assert plugin.GCS_DEST.conn is GCSConn
    assert plugin.GCS_DEST.dest is GCSDest
    assert plugin.GCS_DEST.plugin is plugin.GCSDestPlugin
    assert plugin.GCS_DEST.explorer is plugin.GCSExplorer
    assert plugin.GCS_DEST.dest_type == "gcs"


def test_gcs_cardinality_uses_paths_length():
    src = GCSSrc(conn=_conn(), paths=["a", "b", "c"])
    assert plugin.GCS_SRC.cardinality(src) == 3
    dest = GCSDest(conn=_conn(), paths=["a", "b"])
    assert plugin.GCS_DEST.cardinality(dest) == 2
