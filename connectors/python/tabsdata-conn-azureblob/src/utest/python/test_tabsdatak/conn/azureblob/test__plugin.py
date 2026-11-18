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
from tabsdatax.itest.fixture.backends.azurebloblocal import (
    AzureBlobLocalDomain,
    azurebloblocal_domain,
)

# noinspection PyProtectedMember
import tabsdatak.conn.azureblob._plugin as plugin
from tabsdatak._handoff import DestinationContract
from tabsdatak.api import Secret
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin, _target
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import AzureAccountKey

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
# credentials
# ---------------------------------------------------------------------------


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
        conn_cfg={
            "obstore.endpoint": "http://127.0.0.1:10000",
            "obstore.allow_http": True,
        },
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


def test_dest_plugin_writes_through_the_connection_store(monkeypatch, tmp_path):
    captured: dict = {}
    monkeypatch.setattr(
        _target, "write_files", lambda **options: captured.update(options)
    )
    conn = _dest_conn()
    table = pl.LazyFrame({"a": [1]})
    plugin.AzureBlobDestPlugin().write_out(
        _dest_ctx(tmp_path), conn, AzureBlobDest(paths=["out/x.parquet"]), [table]
    )
    assert captured["tables"][0] is table
    assert captured["store"] == plugin.file_store(conn)
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


def test_dest_plugin_takes_lazy_frames_or_none():
    assert DestinationContract(plugin.AzureBlobDestPlugin).frames()


def test_dest_plugin_hands_none_tables_to_the_writer(monkeypatch, tmp_path):
    captured: dict = {}
    monkeypatch.setattr(
        _target, "write_files", lambda **options: captured.update(options)
    )
    table = pl.LazyFrame({"a": [1]})
    plugin.AzureBlobDestPlugin().write_out(
        _dest_ctx(tmp_path),
        _dest_conn(),
        AzureBlobDest(paths=["out/x.parquet", "out/y.parquet"]),
        [None, table],
    )
    assert captured["tables"] == [None, table]
    assert captured["cfg"].paths == ["out/x.parquet", "out/y.parquet"]


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
