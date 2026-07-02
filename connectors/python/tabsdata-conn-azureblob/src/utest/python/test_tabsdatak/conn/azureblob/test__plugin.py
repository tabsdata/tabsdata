from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.api import Secret
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin
# noinspection PyProtectedMember
from tabsdatak.common._transporter import _transporter1
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.azureblob import AzureBlobDest, AzureBlobDestConn, AzureBlobSrc, AzureBlobSrcConn
from tabsdatak.spi import TableFileInput
# noinspection PyProtectedMember
import tabsdatak.conn.azureblob._plugin as plugin



pytestmark = pytest.mark.azureblob


def _az_creds() -> AzureAccountKey:
    return AzureAccountKey(
        account_name=Secret("acctname"),
        account_key=Secret("acctkey"),
    )


def _src_conn(**overrides) -> AzureBlobSrcConn:
    kw = dict(
        container="my-container",
        credentials=_az_creds(),
    )
    kw.update(overrides)
    return AzureBlobSrcConn(**kw)


def _dest_conn(**overrides) -> AzureBlobDestConn:
    kw = dict(
        container="my-container",
        credentials=_az_creds(),
    )
    kw.update(overrides)
    return AzureBlobDestConn(**kw)


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_emits_transporter_azure_with_env_credentials():
    loc = plugin.build_location(_src_conn(), "raw/users.parquet")
    assert isinstance(loc, _transporter1.TransporterAzure)
    assert loc.url == "az://my-container/raw/users.parquet"
    assert loc.account_name == _transporter1.TransporterEnv(
        _transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME,
    )
    assert loc.account_key == _transporter1.TransporterEnv(
        _transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY,
    )


def test_build_location_strips_leading_slash_on_path_entry():
    loc = plugin.build_location(_src_conn(), "/raw/users.parquet")
    assert loc.url == "az://my-container/raw/users.parquet"


def test_build_location_preserves_explicit_az_uri():
    loc = plugin.build_location(_src_conn(), "az://other-container/x/y.parquet")
    assert loc.url == "az://other-container/x/y.parquet"


def test_build_location_preserves_explicit_abfss_uri():
    loc = plugin.build_location(
        _src_conn(), "abfss://other@acct.dfs.core.windows.net/x/y.parquet",
    )
    assert loc.url == "abfss://other@acct.dfs.core.windows.net/x/y.parquet"


# ---------------------------------------------------------------------------
# _run_env / _credentials
# ---------------------------------------------------------------------------


def test_run_env_resolves_credentials_to_server_side_env_var_names():
    env = plugin.run_env(_src_conn())
    assert env == {
        _transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME: "acctname",
        _transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY: "acctkey",
    }


def test_credentials_rejects_non_account_key():
    class _OtherCreds:
        pass

    conn = MagicMock(spec=AzureBlobSrcConn)
    conn.credentials = _OtherCreds()
    with pytest.raises(TypeError, match="must be AzureAccountKey"):
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
        return [[Path(tmp_path / "ok.parquet")]]

    monkeypatch.setattr(_file_plugin, "import_files", fake)

    conn = _src_conn()
    src = AzureBlobSrc(paths=["a/b.parquet"])
    out = plugin.AzureBlobSrcPlugin().read_in(_src_ctx(tmp_path), conn, src)

    assert out == [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]
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

    conn = _dest_conn()
    dest = AzureBlobDest(paths=["out/x.parquet"])
    parquet = Path(tmp_path / "in.parquet")
    plugin.AzureBlobDestPlugin().write_out(_dest_ctx(tmp_path), conn, dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    src_exp = plugin.AzureBlobSrcExplorer()
    assert src_exp.metadata(_src_conn()) is None
    assert src_exp.data(_src_conn(), "SELECT 1") is None
    dest_exp = plugin.AzureBlobDestExplorer()
    assert dest_exp.metadata(_dest_conn()) is None
    assert dest_exp.data(_dest_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_azureblob_src_def_wires_plugin_and_explorer():
    assert plugin.AZURE_BLOB_SRC.conn is AzureBlobSrcConn
    assert plugin.AZURE_BLOB_SRC.src is AzureBlobSrc
    assert plugin.AZURE_BLOB_SRC.plugin is plugin.AzureBlobSrcPlugin
    assert plugin.AZURE_BLOB_SRC.explorer is plugin.AzureBlobSrcExplorer
    assert plugin.AZURE_BLOB_SRC.type_ == "az-file-in"


def test_azureblob_dest_def_wires_plugin_and_explorer():
    assert plugin.AZURE_BLOB_DEST.conn is AzureBlobDestConn
    assert plugin.AZURE_BLOB_DEST.dest is AzureBlobDest
    assert plugin.AZURE_BLOB_DEST.plugin is plugin.AzureBlobDestPlugin
    assert plugin.AZURE_BLOB_DEST.explorer is plugin.AzureBlobDestExplorer
    assert plugin.AZURE_BLOB_DEST.type_ == "az-file-out"


def test_azureblob_cardinality_uses_paths_length():
    src = AzureBlobSrc(paths=["a", "b", "c"])
    assert plugin.AZURE_BLOB_SRC.cardinality(src) == 3
    dest = AzureBlobDest(paths=["a", "b"])
    assert plugin.AZURE_BLOB_DEST.cardinality(dest) == 2
