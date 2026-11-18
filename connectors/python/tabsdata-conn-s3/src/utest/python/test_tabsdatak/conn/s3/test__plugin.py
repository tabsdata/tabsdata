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
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain, s3local_domain

# noinspection PyProtectedMember
import tabsdatak.conn.s3._plugin as plugin
from tabsdatak._handoff import DestinationContract
from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin, _target
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.conn.s3._explorer import S3ConnectionExplorer

pytestmark = pytest.mark.s3


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


def test_a_connection_maps_to_a_store_of_its_bucket_prefix_and_credentials():
    conn = _src_conn(
        base_path="/landing/raw",
        conn_cfg={
            "obstore.endpoint": "http://127.0.0.1:9000",
            "obstore.allow_http": True,
        },
    )
    store = plugin.file_store(conn)
    assert store.root == "s3://my-bucket/landing/raw"
    assert store.options == {
        "aws_access_key_id": "AKIA0000000000000000",
        "aws_secret_access_key": "secret-value",
        "aws_region": "us-east-1",
        "aws_endpoint": "http://127.0.0.1:9000",
        "aws_allow_http": "true",
    }
    assert plugin.file_store(_src_conn()).root == "s3://my-bucket"


@s3local_domain("s3local")
def test_src_plugin_reads_s3local_objects_lazily_naming_their_uri(
    domain_s3local: S3LocalDomain, tmp_path
):
    prefix = f"plugin-{uuid.uuid4().hex}"
    options = dict(domain_s3local.storage_options)
    root = f"{domain_s3local.uri.rstrip('/')}/{prefix}"
    store = FileStore(root, options)
    for name in ("a.parquet", "b.parquet"):
        content = io.BytesIO()
        pl.DataFrame({"name": [name]}).write_parquet(content)
        obstore.put(store.store(), f"in/{name}", content.getvalue())
    base = root.removeprefix(f"s3://{domain_s3local.bucket}")
    conn = S3SrcConn(
        bucket=domain_s3local.bucket,
        region=domain_s3local.region,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(domain_s3local.access_key),
            secret_access_key=Secret(domain_s3local.secret_key),
        ),
        base_path=base,
        conn_cfg={f"obstore.{key}": value for key, value in options.items()},
    )
    try:
        (slot,) = plugin.S3SrcPlugin().read_in(
            _src_ctx(tmp_path), conn, S3Src(paths=["in/*.parquet"])
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
    plugin.S3DestPlugin().write_out(
        _dest_ctx(tmp_path), conn, S3Dest(paths=["out/x.parquet"]), [table]
    )
    assert captured["tables"][0] is table
    assert captured["store"] == plugin.file_store(conn)
    assert captured["errors"] is plugin.ERRORS
    assert captured["cfg"].paths == ["out/x.parquet"]


def test_dest_plugin_takes_lazy_frames_or_none():
    assert DestinationContract(plugin.S3DestPlugin).frames()


def test_dest_plugin_hands_none_tables_to_the_writer(monkeypatch, tmp_path):
    captured: dict = {}
    monkeypatch.setattr(
        _target, "write_files", lambda **options: captured.update(options)
    )
    table = pl.LazyFrame({"a": [1]})
    plugin.S3DestPlugin().write_out(
        _dest_ctx(tmp_path),
        _dest_conn(),
        S3Dest(paths=["out/x.parquet", "out/y.parquet"]),
        [None, table],
    )
    assert captured["tables"] == [None, table]
    assert captured["cfg"].paths == ["out/x.parquet", "out/y.parquet"]


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


def test_reader_conf_carries_the_drop_metadata_src_cfg_key():
    src = S3Src(
        paths=["a/b.parquet"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    rc = _file_plugin.ReaderConf.from_src(object(), src)
    assert rc.drop_metadata is True
