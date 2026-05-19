"""End-to-end MongoDB integration tests: invoke the destination plugin
against a real MongoDB server.

The ``mongodb_config`` fixture (in :mod:`conftest`) auto-starts a
``mongo`` docker container on a random host port and exposes its
connection details to the tests below -- no environment-variable setup
required when docker is available. Each test scopes its writes to a
fresh, uuid-tagged collection name under the container's
``td_integ`` database so concurrent runs don't trample each other.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

# noinspection PyProtectedMember
import tabsdatak.conn.mongodb._plugin as plugin
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBConn, MongoDBDest
from tabsdatak.spi import ParquetFile

pytestmark = pytest.mark.mongodb


# ---------------------------------------------------------------------------
# DestContext shim
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    def work_dir(self) -> Path:
        return self._work

    @property
    def exec_plan_id(self) -> str:
        return "mongo-integ-exec"

    @property
    def trx_id(self) -> str:
        return "mongo-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(mongodb_config) -> MongoDBConn:
    return MongoDBConn(
        uri=mongodb_config.uri,
        credentials=UserPassword(
            user=Secret.lit(mongodb_config.user),
            password=Secret.lit(mongodb_config.password),
        ),
    )


@pytest.fixture
def database(mongodb_config) -> str:
    return mongodb_config.database


@pytest.fixture
def collection_name() -> str:
    """Per-test, unique collection name (kept short -- ``uuid4().hex``
    is 32 chars, well under MongoDB's 235-byte cap)."""
    return f"td_integ_{uuid.uuid4().hex}"


@pytest.fixture
def fqn(database: str, collection_name: str) -> str:
    return f"{database}.{collection_name}"


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


@pytest.fixture
def cleanup(conn: MongoDBConn, database: str, collection_name: str):
    """Drop the collection after the test."""
    yield
    import pymongo

    client = pymongo.MongoClient(plugin._uri_with_credentials(conn))
    try:
        client[database].drop_collection(collection_name)
    finally:
        client.close()


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def _read_collection(
    conn: MongoDBConn, database: str, collection_name: str
) -> list[dict]:
    import pymongo

    client = pymongo.MongoClient(plugin._uri_with_credentials(conn))
    try:
        coll = client[database][collection_name]
        return list(coll.find({}, projection={"_id": False}))
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dest_inserts_documents_without_id_field(
    conn, fqn, database, collection_name, work_dir, tmp_path, cleanup,
):
    parquet = _make_parquet(
        tmp_path / "users.parquet",
        [{"name": "alice", "age": 30}, {"name": "bob", "age": 31}],
    )
    dest = MongoDBDest(
        conn=conn,
        collections_with_ids=[(fqn, None)],
    )
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir), dest, [ParquetFile(parquet)],
    )

    docs = sorted(
        _read_collection(conn, database, collection_name),
        key=lambda d: d["name"],
    )
    assert docs == [
        {"name": "alice", "age": 30},
        {"name": "bob", "age": 31},
    ]


def test_dest_upserts_documents_with_id_field(
    conn, fqn, database, collection_name, work_dir, tmp_path, cleanup,
):
    # First write -- inserts both rows.
    p1 = _make_parquet(
        tmp_path / "v1.parquet",
        [
            {"user_id": 1, "name": "alice"},
            {"user_id": 2, "name": "bob"},
        ],
    )
    dest = MongoDBDest(
        conn=conn,
        collections_with_ids=[(fqn, "user_id")],
    )
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir), dest, [ParquetFile(p1)],
    )

    # Second write -- updates user 1, inserts user 3.
    p2 = _make_parquet(
        tmp_path / "v2.parquet",
        [
            {"user_id": 1, "name": "alice2"},
            {"user_id": 3, "name": "carol"},
        ],
    )
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir), dest, [ParquetFile(p2)],
    )

    docs = sorted(
        _read_collection(conn, database, collection_name),
        key=lambda d: d["user_id"],
    )
    assert [d["user_id"] for d in docs] == [1, 2, 3]
    by_id = {d["user_id"]: d["name"] for d in docs}
    assert by_id[1] == "alice2"  # updated
    assert by_id[2] == "bob"     # untouched
    assert by_id[3] == "carol"   # inserted


def test_dest_replace_strategy_drops_old_documents(
    conn, fqn, database, collection_name, work_dir, tmp_path, cleanup,
):
    # Seed with one document.
    p1 = _make_parquet(tmp_path / "seed.parquet", [{"name": "seed"}])
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir),
        MongoDBDest(
            conn=conn,
            collections_with_ids=[(fqn, None)],
        ),
        [ParquetFile(p1)],
    )

    # Replace.
    p2 = _make_parquet(tmp_path / "replace.parquet", [{"name": "fresh"}])
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir),
        MongoDBDest(
            conn=conn,
            collections_with_ids=[(fqn, None)],
            if_collection_exists="replace",
        ),
        [ParquetFile(p2)],
    )

    docs = _read_collection(conn, database, collection_name)
    assert docs == [{"name": "fresh"}]


def test_dest_chunked_writes(
    conn, fqn, database, collection_name, work_dir, tmp_path, cleanup,
):
    parquet = _make_parquet(
        tmp_path / "u.parquet",
        [{"i": i} for i in range(10)],
    )
    dest = MongoDBDest(
        conn=conn,
        collections_with_ids=[(fqn, None)],
        docs_per_trx=3,
    )
    plugin._MongoDBDestPlugin().write_out(
        _Ctx(work_dir), dest, [ParquetFile(parquet)],
    )

    docs = sorted(_read_collection(conn, database, collection_name), key=lambda d: d["i"])
    assert [d["i"] for d in docs] == list(range(10))
