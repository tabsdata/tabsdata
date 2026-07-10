
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pymongo
import pytest
from pymongo.errors import BulkWriteError, ConnectionFailure, PyMongoError

# noinspection PyProtectedMember
import tabsdatak.conn.mongodb._plugin as plugin
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBDestConn, MongoDBDest
from tabsdatak.conn.mongodb._explorer._explorer import MongoDBConnectionExplorer
from tabsdatak.conn.mongodb.error import (
    MongodbErrorCode,
    MongoDBRuntimeException,
)

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
        return "mongo-ut-exec"

    @property
    def trx_id(self) -> str:
        return "mongo-ut-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


def _bare_conn() -> MongoDBDestConn:
    """A MongoDBDestConn whose URI never reaches a real driver -- the
    pymongo client is monkeypatched in every test."""
    return MongoDBDestConn(
        uri="mongodb://stub.invalid:27017",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Mock MongoClient infrastructure
# ---------------------------------------------------------------------------


class _FakeSession:
    """Drop-in stand-in for ``pymongo.ClientSession`` capturing the
    txn lifecycle the plugin drives."""

    def __init__(self) -> None:
        self.started_transactions = 0
        self.committed = 0
        self.aborted = 0
        self.entered = False
        self.exited = False

    def __enter__(self) -> "_FakeSession":
        self.entered = True
        return self

    def __exit__(self, *exc) -> None:
        self.exited = True

    def start_transaction(self) -> None:
        self.started_transactions += 1

    def commit_transaction(self) -> None:
        self.committed += 1

    def abort_transaction(self) -> None:
        self.aborted += 1


class _FakeCollection:
    """Captures bulk_write calls and the count_documents/drop sequence."""

    def __init__(self, name: str, exists: bool = False) -> None:
        self.name = name
        self._exists = exists
        self.dropped = False
        self.bulk_write_calls: list[dict] = []
        self.inserted_one: list[dict] = []
        self.deleted_one_filters: list[dict] = []

    def count_documents(self, flt: dict) -> int:
        return 1 if self._exists else 0

    def drop(self) -> None:
        self.dropped = True
        self._exists = False

    def insert_one(self, doc: dict) -> None:
        self.inserted_one.append(doc)
        self._exists = True

    def delete_one(self, flt: dict) -> None:
        self.deleted_one_filters.append(flt)

    def bulk_write(self, ops, session=None, ordered=False):
        self.bulk_write_calls.append({
            "ops": list(ops),
            "session": session,
            "ordered": ordered,
        })


class _FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, _FakeCollection] = {}

    def get_collection(self, name: str) -> _FakeCollection:
        if name not in self.collections:
            self.collections[name] = _FakeCollection(name)
        return self.collections[name]


class _FakeAdmin:
    def __init__(self, ping_raises: Exception | None = None) -> None:
        self._raises = ping_raises
        self.commands: list[str] = []

    def command(self, name: str, *a, **kw) -> dict:
        self.commands.append(name)
        if self._raises is not None:
            raise self._raises
        return {"ok": 1}


class _FakeClient:
    """A test double for ``pymongo.MongoClient`` that exposes the
    surface the plugin uses: ``admin.command``, ``client[db]``,
    ``start_session``, ``close``."""

    def __init__(self, ping_raises: Exception | None = None) -> None:
        self.databases: dict[str, _FakeDatabase] = {}
        self.admin = _FakeAdmin(ping_raises=ping_raises)
        self.session = _FakeSession()
        self.closed = False

    def __getitem__(self, db: str) -> _FakeDatabase:
        if db not in self.databases:
            self.databases[db] = _FakeDatabase()
        return self.databases[db]

    def start_session(self) -> _FakeSession:
        return self.session

    def close(self) -> None:
        self.closed = True


def _install_client(monkeypatch, client: _FakeClient) -> _FakeClient:
    """Make ``pymongo.MongoClient(...)`` return ``client``."""
    captured = {}

    def factory(uri, **kwargs):
        captured["uri"] = uri
        captured["kwargs"] = kwargs
        return client

    monkeypatch.setattr(pymongo, "MongoClient", factory)
    client._captured = captured  # type: ignore[attr-defined]
    return client


# ---------------------------------------------------------------------------
# write_out: happy paths
# ---------------------------------------------------------------------------


def test_write_out_inserts_when_id_field_none(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [{"name": "alice", "age": 30}, {"name": "bob", "age": 31}],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = client["app"].collections["users"]
    assert len(coll.bulk_write_calls) == 1
    ops = coll.bulk_write_calls[0]["ops"]
    assert len(ops) == 2
    assert all(isinstance(op, pymongo.InsertOne) for op in ops)
    assert client.closed is True


def test_write_out_upserts_when_id_field_provided(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [
            {"user_id": 1, "name": "alice"},
            {"user_id": 2, "name": "bob"},
        ],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", "user_id")],
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = client["app"].collections["users"]
    ops = coll.bulk_write_calls[0]["ops"]
    assert len(ops) == 2
    assert all(isinstance(op, pymongo.UpdateOne) for op in ops)


def test_write_out_inserts_when_id_field_and_update_existing_false(
    monkeypatch, work_dir,
):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [{"user_id": 1, "name": "alice"}],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", "user_id")],
        update_existing=False,
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = client["app"].collections["users"]
    ops = coll.bulk_write_calls[0]["ops"]
    assert len(ops) == 1
    assert isinstance(ops[0], pymongo.InsertOne)


def test_write_out_replace_drops_existing_collection(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    # Pre-populate the fake collection so count_documents() returns 1
    db = client["app"]
    db.collections["users"] = _FakeCollection("users", exists=True)
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [{"name": "alice"}],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        if_collection_exists="replace",
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = db.collections["users"]
    assert coll.dropped is True
    # The replace path inserts a sentinel + deletes it -- regression
    # guard for the "create collection outside a txn" trick.
    assert coll.inserted_one  # sentinel
    assert coll.deleted_one_filters


def test_write_out_chunks_by_docs_per_trx(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [{"name": f"u{i}"} for i in range(7)],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        docs_per_trx=3,
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = client["app"].collections["users"]
    sizes = [len(c["ops"]) for c in coll.bulk_write_calls]
    # 7 docs at chunk size 3 -> [3, 3, 1].
    assert sizes == [3, 3, 1]


def test_write_out_uses_transactions_when_enabled(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(
        work_dir / "users.parquet",
        [{"name": "alice"}, {"name": "bob"}],
    )
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        use_trxs=True,
        docs_per_trx=1,
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    # 2 docs at chunk size 1 -> 2 batches -> 2 transactions.
    assert client.session.started_transactions == 2
    assert client.session.committed == 2
    assert client.session.aborted == 0
    # The bulk_write call should have received the session.
    coll = client["app"].collections["users"]
    assert all(
        c["session"] is client.session for c in coll.bulk_write_calls
    )


def test_write_out_multiple_collections(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    p1 = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    p2 = _make_parquet(work_dir / "o.parquet", [{"x": 1}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None), ("app.orders", None)],
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [p1, p2])

    assert "users" in client["app"].collections
    assert "orders" in client["app"].collections


# ---------------------------------------------------------------------------
# write_out: error paths
# ---------------------------------------------------------------------------


def test_write_out_raises_mongodb_18_on_count_mismatch(monkeypatch, work_dir):
    _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None), ("app.orders", None)],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_18


def test_write_out_raises_mongodb_19_on_connect_failure(monkeypatch, work_dir):
    _install_client(
        monkeypatch,
        _FakeClient(ping_raises=ConnectionFailure("bad host")),
    )
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_19


def test_write_out_raises_mongodb_22_on_bare_collection_name(
    monkeypatch, work_dir,
):
    _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        # "users" without a leading database -- legal as a collection
        # name validator-wise, but not legal at runtime.
        collections_with_ids=[("users", None)],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_22


def test_write_out_raises_mongodb_21_when_id_field_missing(
    monkeypatch, work_dir,
):
    _install_client(monkeypatch, _FakeClient())
    # Document is missing the declared id_field 'user_id'.
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "alice"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", "user_id")],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_21


def test_write_out_raises_mongodb_20_on_pymongo_write_failure(
    monkeypatch, work_dir,
):
    _install_client(monkeypatch, _FakeClient())
    # Cause the bulk_write call to raise.
    def explode(*a, **kw):
        raise PyMongoError("write boom")
    monkeypatch.setattr(
        _FakeCollection, "bulk_write", lambda self, *a, **kw: explode(),
    )
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_20


def test_write_out_swallows_dup_keys_when_configured(monkeypatch, work_dir):
    _install_client(monkeypatch, _FakeClient())

    def explode(self, *a, **kw):
        raise BulkWriteError({
            "writeErrors": [{"code": 11000, "errmsg": "dup"}],
        })
    monkeypatch.setattr(_FakeCollection, "bulk_write", explode)

    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        fail_on_duplicate_key=False,
    )
    # Should NOT raise -- the dup-key error is swallowed.
    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])


def test_write_out_propagates_non_dup_key_bulk_errors(monkeypatch, work_dir):
    _install_client(monkeypatch, _FakeClient())

    def explode(self, *a, **kw):
        raise BulkWriteError({
            "writeErrors": [{"code": 9999, "errmsg": "other"}],
        })
    monkeypatch.setattr(_FakeCollection, "bulk_write", explode)

    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        fail_on_duplicate_key=False,
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_20


def test_write_out_aborts_transaction_on_failure(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())

    def explode(self, *a, **kw):
        raise PyMongoError("write boom")
    monkeypatch.setattr(_FakeCollection, "bulk_write", explode)

    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        collections_with_ids=[("app.users", None)],
        use_trxs=True,
    )
    with pytest.raises(MongoDBRuntimeException):
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert client.session.aborted == 1
    assert client.session.committed == 0


# ---------------------------------------------------------------------------
# URI / pymongo-kwargs / split-collection helpers
# ---------------------------------------------------------------------------


# uri resolution coverage moved to _convert tests in
# tabsdata-conn-common (test_types.test_convert_*).


def test_uri_with_credentials_inlines_user_pass():
    conn = MongoDBDestConn(
        uri="mongodb://host:27017",
        credentials=UserPassword(
            user=Secret("us er"),
            password=Secret("p@ss"),
        ),
    )
    # ' ' -> '+', '@' -> '%40' (quote_plus).
    assert plugin._uri_with_credentials(conn) == (
        "mongodb://us+er:p%40ss@host:27017"
    )


def test_pymongo_kwargs_strips_prefix():
    conn = MongoDBDestConn(
        uri="mongodb://host:27017",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
        conn_cfg={
            "pymongo.tls": True,
            "pymongo.serverSelectionTimeoutMS": 1000,
            "other.something": "ignored",
        },
    )
    assert plugin._pymongo_kwargs(conn) == {
        "tls": True,
        "serverSelectionTimeoutMS": 1000,
    }


def test_split_collection_ok():
    assert plugin._split_collection("app.users") == ("app", "users")


def test_split_collection_rejects_bare_name():
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._split_collection("users")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_22


def test_split_collection_rejects_empty_db():
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._split_collection(".users")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_22


# ---------------------------------------------------------------------------
# Binding wiring
# ---------------------------------------------------------------------------


def test_mongodb_dest_def_wires_plugin_and_explorer():
    assert plugin.MONGODB_DEST.conn is MongoDBDestConn
    assert plugin.MONGODB_DEST.dest is MongoDBDest
    assert plugin.MONGODB_DEST.plugin is plugin._MongoDBDestPlugin
    assert plugin.MONGODB_DEST.explorer is MongoDBConnectionExplorer
    assert plugin.MONGODB_DEST.type_ == "mongodb-out"


def test_mongodb_dest_cardinality_uses_collections_length():
    dest = MongoDBDest(
        collections_with_ids=[("a.b", None), ("c.d", None)],
    )
    assert plugin.MONGODB_DEST.cardinality(dest) == 2


# ---------------------------------------------------------------------------
# _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_dest_cfg_returns_defaults_when_empty():
    import logging as _logging
    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg({"mongodb.logging_level": 20})
    assert cfg.logging_level == 20


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)
