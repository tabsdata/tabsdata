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
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain, gcslocal_domain

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin, _target

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn, GCSDest, GCSSrc
from tabsdatak.conn.gcs._explorer import GCSConnectionExplorer
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH

# noinspection PyProtectedMember
import tabsdatak.conn.gcs._plugin as plugin

pytestmark = pytest.mark.gcs



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
# credentials
# ---------------------------------------------------------------------------




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


def test_a_connection_maps_to_a_store_of_its_bucket_prefix_and_key():
    conn = _src_conn(
        base_path="/landing/raw",
        conn_cfg={"obstore.allow_http": True},
    )
    store = plugin.file_store(conn)
    assert store.root == "gs://my-bucket/landing/raw"
    assert store.options == {
        "google_service_account_key": '{"type":"service_account"}',
        "google_allow_http": "true",
    }
    assert plugin.file_store(_src_conn()).root == "gs://my-bucket"


@gcslocal_domain("gcslocal")
def test_src_plugin_reads_gcslocal_objects_lazily_naming_their_uri(
    domain_gcslocal: GcsLocalDomain, tmp_path
):
    prefix = f"plugin-{uuid.uuid4().hex}"
    options = dict(domain_gcslocal.storage_options)
    root = f"{domain_gcslocal.uri.rstrip('/')}/{prefix}"
    store = FileStore(root, options)
    for name in ("a.parquet", "b.parquet"):
        content = io.BytesIO()
        pl.DataFrame({"name": [name]}).write_parquet(content)
        obstore.put(store.store(), f"in/{name}", content.getvalue())
    conn = GCSSrcConn(
        bucket=root.removeprefix("gs://").partition("/")[0],
        credentials=GCPJson(json=Secret(options["google_service_account_key"])),
        base_path="/" + root.removeprefix("gs://").partition("/")[2],
        conn_cfg={f"obstore.{key}": value for key, value in options.items()},
    )
    try:
        (slot,) = plugin.GCSSrcPlugin().read_in(
            _src_ctx(tmp_path), conn, GCSSrc(paths=["in/*.parquet"])
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
    plugin.GCSDestPlugin().write_out(
        _dest_ctx(tmp_path), conn, GCSDest(paths=["out/x.parquet"]), [table]
    )
    assert captured["tables"][0] is table
    assert captured["store"] == plugin.file_store(conn)
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
