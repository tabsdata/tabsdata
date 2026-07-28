#
# Copyright 2026 Tabsdata Inc.
#


from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    MongoDBErrorCode,
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
    def __init__(self, name: str = "") -> None:
        self.name = name
        self.collections: dict[str, _FakeCollection] = {}
        self.last_codec_options: Any = None

    def get_collection(
        self, name: str, codec_options: Any = None
    ) -> _FakeCollection:
        self.last_codec_options = codec_options
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

    def __init__(
        self,
        ping_raises: Exception | None = None,
        default_db: str | None = None,
    ) -> None:
        self.databases: dict[str, _FakeDatabase] = {}
        self.admin = _FakeAdmin(ping_raises=ping_raises)
        self.session = _FakeSession()
        self.closed = False
        self._default_db = default_db

    def __getitem__(self, db: str) -> _FakeDatabase:
        if db not in self.databases:
            self.databases[db] = _FakeDatabase(db)
        return self.databases[db]

    def get_default_database(self, default: Any = None) -> Any:
        if self._default_db is None:
            return default
        return self[self._default_db]

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
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_18


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
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_19


def test_write_out_raises_mongodb_22_on_bare_collection_name(
    monkeypatch, work_dir,
):
    _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "a"}])
    dest = MongoDBDest(
        # "users" without a leading database -- legal as a collection
        # name validator-wise, and not resolvable at runtime because
        # this connection URI carries no default database.
        collections_with_ids=[("users", None)],
    )
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._MongoDBDestPlugin().write_out(
            _Ctx(work_dir), _bare_conn(), dest, [parquet],
        )
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_22


def test_write_out_bare_collection_uses_uri_default_db(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient(default_db="app"))
    parquet = _make_parquet(work_dir / "u.parquet", [{"name": "alice"}])
    dest = MongoDBDest(
        # Bare "users" -- resolved against the URI's default database.
        collections_with_ids=[("users", None)],
    )

    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])

    coll = client["app"].collections["users"]
    assert len(coll.bulk_write_calls) == 1


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
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_21


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
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_20


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
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_20


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
    assert plugin._split_collection("app.users", None) == ("app", "users")


def test_split_collection_bare_uses_default_db():
    assert plugin._split_collection("users", "app") == ("app", "users")


def test_split_collection_qualified_overrides_default_db():
    assert plugin._split_collection("db2.users", "app") == ("db2", "users")


def test_split_collection_rejects_bare_name_without_default_db():
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._split_collection("users", None)
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_22


def test_split_collection_rejects_empty_db():
    with pytest.raises(MongoDBRuntimeException) as ei:
        plugin._split_collection(".users", "app")
    assert ei.value.api_error is MongoDBErrorCode.MONGODB_22


def test_default_database_name_from_uri():
    assert plugin._default_database_name(_FakeClient(default_db="app")) == "app"


def test_default_database_name_none_when_uri_has_no_db():
    assert plugin._default_database_name(_FakeClient()) is None


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


# ---------------------------------------------------------------------------
# Decimal -> Decimal128 codec
# ---------------------------------------------------------------------------


def test_codec_options_encodes_decimal128():
    import decimal

    import bson
    from bson.decimal128 import Decimal128

    opts = plugin._codec_options()
    doc = {
        "gnp": decimal.Decimal("193.00"),
        "nested": {"rate": decimal.Decimal("1.5")},
        "lst": [decimal.Decimal("2.25")],
        "none": None,
    }
    raw = bson.encode(doc, codec_options=opts)  # InvalidDocument before the fix
    wire = bson.decode(raw)
    assert isinstance(wire["gnp"], Decimal128)
    assert isinstance(wire["nested"]["rate"], Decimal128)
    assert isinstance(wire["lst"][0], Decimal128)
    assert wire["none"] is None
    assert bson.decode(raw, codec_options=opts)["gnp"] == decimal.Decimal("193.00")


def test_codec_options_encodes_date_at_midnight_utc():
    import datetime

    import bson

    raw = bson.encode(
        {"d": datetime.date(2026, 7, 14)}, codec_options=plugin._codec_options()
    )
    assert bson.decode(raw)["d"] == datetime.datetime(
        2026, 7, 14, 0, 0, tzinfo=datetime.timezone.utc
    ).replace(tzinfo=None)


def test_codec_options_encodes_time_on_the_sentinel_date():
    import datetime

    import bson

    raw = bson.encode(
        {"t": datetime.time(9, 30, 15)}, codec_options=plugin._codec_options()
    )
    # BSON has no time-of-day type, so the time rides the sentinel date.
    assert bson.decode(raw)["t"] == datetime.datetime(1970, 1, 1, 9, 30, 15)
    assert plugin.SENTINEL_DATE == datetime.date(1970, 1, 1)


def test_codec_options_encodes_timedelta_as_nanoseconds():
    import datetime

    import bson

    raw = bson.encode(
        {"u": datetime.timedelta(seconds=90)}, codec_options=plugin._codec_options()
    )
    # Nanoseconds, matching what the SQL connectors write for a duration.
    assert bson.decode(raw)["u"] == 90_000_000_000


def test_codec_options_leaves_a_datetime_alone():
    import datetime

    import bson

    stamp = datetime.datetime(2026, 7, 14, 9, 30, 15)
    raw = bson.encode({"ts": stamp}, codec_options=plugin._codec_options())
    assert bson.decode(raw)["ts"] == stamp


def test_write_out_opens_collection_with_decimal_codec(monkeypatch, work_dir):
    client = _install_client(monkeypatch, _FakeClient())
    parquet = _make_parquet(work_dir / "c.parquet", [{"name": "us", "pop": 1}])
    dest = MongoDBDest(collections_with_ids=[("app.country", None)])
    plugin._MongoDBDestPlugin().write_out(_Ctx(work_dir), _bare_conn(), dest, [parquet])
    assert client["app"].last_codec_options is not None
