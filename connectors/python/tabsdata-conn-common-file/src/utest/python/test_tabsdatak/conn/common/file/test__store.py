#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone

import obstore
import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak.common._storage.transfer import TransferError

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore, Stream, forwarded

pytestmark = [pytest.mark.unit]

NAMES = ["a.csv", "b c.csv", "café.csv", "sub/d.csv"]


def content(name: str) -> bytes:
    return f"name\n{name}\n".encode()


def stage(store: FileStore) -> None:
    for name in NAMES:
        obstore.put(store.store(), name, content(name))


def check(store: FileStore) -> None:
    listed = store.listed("")
    assert sorted(file.name for file in listed) == ["a.csv", "b c.csv", "café.csv"]
    assert all(file.last_modified.tzinfo is not None for file in listed)
    assert [file.name for file in store.listed("sub")] == ["d.csv"]
    assert sorted(found["path"] for found in store.walk()) == sorted(NAMES)
    assert [(found["path"], found["size"]) for found in store.walk("sub")] == [
        ("sub/d.csv", len(content("sub/d.csv")))
    ]
    assert list(store.walk("su")) == []
    for name in NAMES:
        stream = store.open(name)
        assert stream.read(3) + stream.read() == content(name)
        stream.close()
    store.delete(["a.csv", "sub/d.csv"])
    assert sorted(file.name for file in store.listed("")) == ["b c.csv", "café.csv"]
    assert store.listed("sub") == []


def test_a_local_folder_lists_reads_and_deletes_its_files(tmp_path):
    store = FileStore(tmp_path.as_uri())
    stage(store)
    check(store)


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_an_emulated_prefix_lists_reads_and_deletes_its_objects(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    emulator,
):
    domain = locals()[f"domain_{emulator}"]
    root = f"{domain.uri.rstrip('/')}/store-{uuid.uuid4().hex}"
    store = FileStore(root, dict(domain.storage_options))
    stage(store)
    frame = pl.DataFrame({"a": [1, 2]})
    parquet = io.BytesIO()
    frame.write_parquet(parquet)
    obstore.put(store.store(), "x.parquet", parquet.getvalue())
    try:
        assert store.scan_parquet("x.parquet").collect().equals(frame)
        store.delete(["x.parquet"])
        check(store)
    finally:
        store.delete(["b c.csv", "café.csv"])


def test_parquet_is_scanned_in_place(tmp_path):
    frame = pl.DataFrame({"a": [1, 2]})
    frame.write_parquet(tmp_path / "x.parquet")
    assert (
        FileStore(tmp_path.as_uri()).scan_parquet("x.parquet").collect().equals(frame)
    )


def test_a_root_ending_in_a_separator_joins_once(tmp_path):
    assert FileStore("file:///").uri("a/b.csv") == "file:///a/b.csv"
    assert FileStore("s3://bucket/p").uri("b.csv") == "s3://bucket/p/b.csv"


def test_an_empty_listing_is_empty(tmp_path):
    assert FileStore(tmp_path.as_uri()).listed("missing") == []


@pytest.mark.parametrize("sizes", [[1] * 20, [7, 7, 7], [-1], [100]])
def test_a_stream_reads_any_sizes_across_chunks(sizes):
    data = bytes(range(20))
    stream = Stream(iter(memoryview(data[i : i + 6]) for i in range(0, 20, 6)))
    read = io.BytesIO()
    for size in sizes:
        read.write(stream.read(size))
    read.write(stream.read())
    assert read.getvalue() == data
    assert stream.read(5) == b""


def test_obstore_keys_in_conn_cfg_are_forwarded_canonical():
    cfg = {
        "obstore.endpoint": "http://127.0.0.1:9000",
        "obstore.allow_http": True,
        "obstore.aws_virtual_hosted_style_request": False,
        "sqlalchemy.echo": True,
    }
    assert forwarded("s3://bucket", cfg) == {
        "aws_endpoint": "http://127.0.0.1:9000",
        "aws_allow_http": "true",
        "aws_virtual_hosted_style_request": "false",
    }
    assert forwarded("s3://bucket", None) == {}


def test_an_unknown_obstore_key_is_refused():
    with pytest.raises(TransferError, match="not a known s3 option"):
        forwarded("s3://bucket", {"obstore.nonsense": "x"})


def test_listed_times_are_instants(tmp_path):
    (tmp_path / "a.csv").write_text("x", encoding="utf-8")
    (listed,) = FileStore(tmp_path.as_uri()).listed("")
    assert abs(listed.last_modified - datetime.now(timezone.utc)).total_seconds() < 60


def ranged_reads(store: FileStore) -> None:
    data = bytes(range(256)) * 40
    obstore.put(store.store(), "r.bin", data)
    ranged = store.ranged("r.bin", 1000)
    assert ranged.read(10) == data[:10]
    assert ranged.seek(-5, io.SEEK_END) == len(data) - 5
    assert ranged.read() == data[-5:]
    assert ranged.read(3) == b""
    assert ranged.seek(995) == 995
    assert ranged.read(1010) == data[995:2005]
    assert ranged.seek(-1005, io.SEEK_CUR) == 1000
    assert ranged.tell() == 1000
    assert ranged.read(-1) == data[1000:]
    with pytest.raises(OSError, match="before the start"):
        ranged.seek(-1)
    ranged.close()


def test_a_ranged_reader_reads_any_part_of_a_local_file(tmp_path):
    ranged_reads(FileStore(tmp_path.as_uri()))


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_a_ranged_reader_reads_any_part_of_an_emulated_object(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    emulator,
):
    domain = locals()[f"domain_{emulator}"]
    root = f"{domain.uri.rstrip('/')}/ranged-{uuid.uuid4().hex}"
    store = FileStore(root, dict(domain.storage_options))
    try:
        ranged_reads(store)
    finally:
        store.delete(["r.bin"])
