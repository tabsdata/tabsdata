#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
import uuid
from urllib.parse import urlparse

import fastavro
import obstore
import openpyxl
import polars as pl
import pytest
from obstore.store import LocalStore
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak.common._storage.transfer import ObjectStores

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store_explorer import FileStoreExplorer

pytestmark = [pytest.mark.unit]

ROWS = [{"id": 1, "name": "ann"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "cy"}]
OPTIONS = {"aws_endpoint": "http://127.0.0.1:1", "aws_allow_http": "true"}


class StoreExplorer(FileStoreExplorer):
    def __init__(self, store: FileStore) -> None:
        self.given = store

    def store(self, conn: object) -> FileStore:
        return self.given


def csv_bytes(rows: list[dict]) -> bytes:
    lines = [",".join(rows[0])] + [",".join(str(v) for v in r.values()) for r in rows]
    return ("\n".join(lines) + "\n").encode()


def ndjson_bytes(rows: list[dict]) -> bytes:
    return "".join(json.dumps(row) + "\n" for row in rows).encode()


def parquet_bytes(rows: list[dict]) -> bytes:
    buffer = io.BytesIO()
    pl.DataFrame(rows).write_parquet(buffer)
    return buffer.getvalue()


def avro_bytes(rows: list[dict]) -> bytes:
    schema = {
        "type": "record",
        "name": "person",
        "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}],
    }
    buffer = io.BytesIO()
    fastavro.writer(buffer, schema, rows)
    return buffer.getvalue()


def xlsx_bytes(rows: list[dict]) -> bytes:
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.append(list(rows[0]))
    for row in rows:
        sheet.append(list(row.values()))
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


STAGED = {
    "people_1.csv": csv_bytes(ROWS),
    "people_2.csv": csv_bytes(ROWS),
    "people_3.csv": csv_bytes(ROWS),
    "deep/er/people.parquet": parquet_bytes(ROWS),
    "people.ndjson": ndjson_bytes(ROWS),
    "people.avro": avro_bytes(ROWS),
    "people.xlsx": xlsx_bytes(ROWS),
}


def explore(parent: FileStore) -> None:
    """Explores ``<parent>/in``, beside a ``<parent>/inside`` it must not see."""
    for name, content in STAGED.items():
        obstore.put(parent.store(), f"in/{name}", content)
    obstore.put(parent.store(), "inside/other.csv", csv_bytes(ROWS))
    store = FileStore(parent.uri("in"), parent.options)
    parsed = urlparse(store.root)
    base = parsed.path.strip("/")
    explorer = StoreExplorer(store)

    assert explorer.check_connect(None) is None

    entities = {e.object_key(): e for e in explorer.metadata(None)}
    assert sorted(entities) == sorted(f"{base}/{name}" for name in STAGED)
    for name, content in STAGED.items():
        entity = entities[f"{base}/{name}"]
        assert entity.scheme == parsed.scheme
        assert entity.bucket == parsed.netloc
        assert entity.fqn() == f"{store.root}/{name}"
        assert entity.size == len(content)
        assert entity.last_modified
        assert entity.file.format == name.rpartition(".")[2]
        if entity.file.format != "xlsx":
            assert entity.field_names() == ["id", "name"]
    shared = [e for e in entities.values() if e.schema_from]
    assert [e.file.format for e in shared] == ["csv"]

    for name in STAGED:
        rows = explorer.data(None, entities[f"{base}/{name}"], limit=2)
        if name.endswith(".csv"):
            assert rows == [{k: str(v) for k, v in row.items()} for row in ROWS[:2]]
        else:
            assert rows == ROWS[:2]


@pytest.fixture
def local_bucket(tmp_path, monkeypatch):
    """A bucket of a cloud scheme kept in a local folder, with the options
    every store of it was built with."""
    built = []

    def build(root: str, options: dict[str, str]) -> LocalStore:
        built.append(options)
        parsed = urlparse(root)
        folder = tmp_path / parsed.netloc / parsed.path.strip("/")
        return LocalStore(folder, mkdir=True)

    monkeypatch.setattr(ObjectStores, "build", staticmethod(build))
    return FileStore(f"s3://bucket-{uuid.uuid4().hex}/landing", OPTIONS), built


def test_a_bucket_is_checked_listed_and_previewed(local_bucket):
    parent, _ = local_bucket
    explore(parent)


def test_every_store_is_built_with_the_options_of_the_connection(local_bucket):
    parent, built = local_bucket
    explore(parent)
    assert built and all(options == OPTIONS for options in built)


def test_an_empty_base_path_checks_and_lists_nothing(local_bucket):
    parent, _ = local_bucket
    explorer = StoreExplorer(FileStore(parent.uri("nothing"), parent.options))
    assert explorer.check_connect(None) is None
    assert explorer.metadata(None) == []


def test_a_preview_stops_at_its_limit_and_at_the_end_of_the_object(local_bucket):
    parent, _ = local_bucket
    obstore.put(parent.store(), "people.csv", csv_bytes(ROWS))
    obstore.put(parent.store(), "empty.parquet", parquet_bytes([]))
    explorer = StoreExplorer(parent)
    entities = {e.file.pattern: e for e in explorer.metadata(None)}
    assert len(explorer.data(None, entities["people.csv"], limit=1)) == 1
    assert len(explorer.data(None, entities["people.csv"], limit=100)) == len(ROWS)
    assert explorer.data(None, entities["empty.parquet"]) == []


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_an_emulated_prefix_is_checked_listed_and_previewed(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    emulator,
):
    domain = locals()[f"domain_{emulator}"]
    root = f"{domain.uri.rstrip('/')}/explorer-{uuid.uuid4().hex}"
    parent = FileStore(root, dict(domain.storage_options))
    try:
        explore(parent)
    finally:
        parent.delete(
            [f"in/{name}" for name in STAGED] + ["inside/other.csv"],
        )


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_a_missing_emulated_bucket_fails_the_check(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    emulator,
):
    domain = locals()[f"domain_{emulator}"]
    scheme = urlparse(domain.uri).scheme
    missing = FileStore(
        f"{scheme}://missing-{uuid.uuid4().hex}", dict(domain.storage_options)
    )
    with pytest.raises(Exception):
        StoreExplorer(missing).check_connect(None)
