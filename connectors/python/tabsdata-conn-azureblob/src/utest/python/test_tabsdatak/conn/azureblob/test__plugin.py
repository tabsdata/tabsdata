#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import obstore
import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain, azurebloblocal_domain

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin

# noinspection PyProtectedMember
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.invoker import transporter1
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.spi import TableFileInput

# noinspection PyProtectedMember
import tabsdatak.conn.azureblob._plugin as plugin

pytestmark = pytest.mark.azureblob

HANDLER = TransporterHandler1()


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
    loc = plugin.build_location(_src_conn(), "raw/users.parquet", HANDLER)
    assert isinstance(loc, transporter1.TransporterAzure)
    assert loc.url == "az://my-container/raw/users.parquet"
    assert loc.account_name == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME,
    )
    assert loc.account_key == transporter1.TransporterEnv(
        transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY,
    )


def test_build_location_strips_leading_slash_on_path_entry():
    loc = plugin.build_location(_src_conn(), "/raw/users.parquet", HANDLER)
    assert loc.url == "az://my-container/raw/users.parquet"


def test_build_location_preserves_explicit_az_uri():
    loc = plugin.build_location(
        _src_conn(), "az://other-container/x/y.parquet", HANDLER
    )
    assert loc.url == "az://other-container/x/y.parquet"


def test_build_location_preserves_explicit_abfss_uri():
    loc = plugin.build_location(
        _src_conn(), "abfss://other@acct.dfs.core.windows.net/x/y.parquet", HANDLER
    )
    assert loc.url == "abfss://other@acct.dfs.core.windows.net/x/y.parquet"


# ---------------------------------------------------------------------------
# _run_env / _credentials
# ---------------------------------------------------------------------------


def test_run_env_resolves_credentials_to_server_side_env_var_names():
    env = plugin.run_env(_src_conn(), HANDLER)
    assert env == {
        transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME: "acctname",
        transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY: "acctkey",
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


def test_a_connection_maps_to_a_store_of_its_container_prefix_and_account():
    conn = _src_conn(
        base_path="/landing/raw",
        conn_cfg={"obstore.endpoint": "http://127.0.0.1:10000", "obstore.allow_http": True},
    )
    store = plugin.file_store(conn)
    assert store.root.endswith("/landing/raw")
    assert store.root.startswith("az://")
    assert set(store.options) == {
        "azure_storage_account_name",
        "azure_storage_account_key",
        "azure_storage_endpoint",
        "azure_allow_http",
    }


@azurebloblocal_domain("azurebloblocal")
def test_src_plugin_reads_azurebloblocal_objects_lazily_naming_their_uri(
    domain_azurebloblocal: AzureBlobLocalDomain, tmp_path
):
    prefix = f"plugin-{uuid.uuid4().hex}"
    options = dict(domain_azurebloblocal.storage_options)
    root = f"{domain_azurebloblocal.uri.rstrip('/')}/{prefix}"
    store = FileStore(root, options)
    for name in ("a.parquet", "b.parquet"):
        content = io.BytesIO()
        pl.DataFrame({"name": [name]}).write_parquet(content)
        obstore.put(store.store(), f"in/{name}", content.getvalue())
    conn = AzureBlobSrcConn(
        container=domain_azurebloblocal.container,
        credentials=AzureAccountKey(
            account_name=Secret(domain_azurebloblocal.account_name),
            account_key=Secret(domain_azurebloblocal.account_key),
        ),
        base_path="/" + root.removeprefix("az://").partition("/")[2],
        conn_cfg={f"obstore.{key}": value for key, value in options.items()},
    )
    try:
        (slot,) = plugin.AzureBlobSrcPlugin().read_in(
            _src_ctx(tmp_path), conn, AzureBlobSrc(paths=["in/*.parquet"])
        )
        tables = list(slot)
        assert [table.frame.collect()["name"].item() for table in tables] == [
            "a.parquet",
            "b.parquet",
        ]
        assert [table.metadata.columns[FILE_META_PATH] for table in tables] == [
            f"{root}/in/a.parquet",
            f"{root}/in/b.parquet",
        ]
    finally:
        store.delete(["in/a.parquet", "in/b.parquet"])


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
    dest = AzureBlobDest(paths=["out/x.parquet"])
    parquet = Path(tmp_path / "in.parquet")
    plugin.AzureBlobDestPlugin().write_out(_dest_ctx(tmp_path), conn, dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["run_env"] is plugin.run_env
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_azureblob_src_def_wires_plugin_and_explorer():
    assert plugin.AZURE_BLOB_SRC.conn is AzureBlobSrcConn
    assert plugin.AZURE_BLOB_SRC.src is AzureBlobSrc
    assert plugin.AZURE_BLOB_SRC.plugin is plugin.AzureBlobSrcPlugin
    assert plugin.AZURE_BLOB_SRC.explorer is AzureBlobConnectionExplorer
    assert plugin.AZURE_BLOB_SRC.type_ == "az-file-in"


def test_azureblob_dest_def_wires_plugin_and_explorer():
    assert plugin.AZURE_BLOB_DEST.conn is AzureBlobDestConn
    assert plugin.AZURE_BLOB_DEST.dest is AzureBlobDest
    assert plugin.AZURE_BLOB_DEST.plugin is plugin.AzureBlobDestPlugin
    assert plugin.AZURE_BLOB_DEST.explorer is AzureBlobConnectionExplorer
    assert plugin.AZURE_BLOB_DEST.type_ == "az-file-out"


def test_azureblob_cardinality_uses_paths_length():
    src = AzureBlobSrc(paths=["a", "b", "c"])
    assert plugin.AZURE_BLOB_SRC.cardinality(src) == 3
    dest = AzureBlobDest(paths=["a", "b"])
    assert plugin.AZURE_BLOB_DEST.cardinality(dest) == 2


def test_reader_conf_carries_the_drop_metadata_src_cfg_key():
    src = AzureBlobSrc(
        paths=["a/b.parquet"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    rc = _file_plugin.ReaderConf.from_src(object(), src)
    assert rc.drop_metadata is True
