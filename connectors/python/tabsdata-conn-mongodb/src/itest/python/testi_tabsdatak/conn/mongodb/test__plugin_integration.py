#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB connector integration tests: drive ``_MongoDBDestPlugin.write_out``
directly against a tdx-provisioned ``mongodb`` domain with a fabricated plugin
context -- unlike ``test__plugin_e2e.py``, which goes through a running tabsdata
instance. MongoDB is destination-only (no ``MongoDBSrc``), so writes land in
uniquely-named scratch collections (dropped on teardown) and are verified by
reading them back with pymongo (the ``mongo`` fixture), never through the
connector.

The fabricated plugin context and the parquet/credential helpers are shared under
:mod:`tabsdatax.itest.fixture.functions` (``files`` / ``sql``); only the mongodb
``Conn`` construction and the document verification are here. Requires a MongoDB
docker container and the ``mongodb`` domain provisioned.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain, mongodb_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import (
    scratch_table,
    sql_credentials,
    write_parquet,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import (
    MongoDBDest,
    MongoDBDestConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.mongodb


def _dest_conn(domain: MongodbDomain) -> MongoDBDestConn:
    return MongoDBDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def _write(domain, ctx, collection, rows, *, if_collection_exists):
    plugin._MongoDBDestPlugin().write_out(
        ctx,
        _dest_conn(domain),
        MongoDBDest(
            collections_with_ids=[(collection, None)],
            if_collection_exists=if_collection_exists,
        ),
        rows,
    )


@mongodb_domain("mongodb")
def test_dest_then_read_round_trip(
    domain_mongodb: MongodbDomain, mongo, tmp_path: Path
):
    db = mongo(domain_mongodb)
    collection = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        _write(
            domain_mongodb,
            PluginDestCtx(work),
            collection,
            [parquet],
            if_collection_exists="replace",
        )
        got = db.read_collection(collection)
        assert_frames_match(
            got, pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
        )
    finally:
        db.drop_collection(collection)


@mongodb_domain("mongodb")
def test_dest_append_accumulates(domain_mongodb: MongodbDomain, mongo, tmp_path: Path):
    db = mongo(domain_mongodb)
    collection = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    try:
        _write(
            domain_mongodb,
            PluginDestCtx(work),
            collection,
            [first],
            if_collection_exists="replace",
        )
        _write(
            domain_mongodb,
            PluginDestCtx(work),
            collection,
            [second],
            if_collection_exists="append",
        )
        assert db.count(collection) == 2
    finally:
        db.drop_collection(collection)


@mongodb_domain("mongodb")
def test_dest_replace_drops_existing(
    domain_mongodb: MongodbDomain, mongo, tmp_path: Path
):
    db = mongo(domain_mongodb)
    collection = scratch_table()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    try:
        _write(
            domain_mongodb,
            PluginDestCtx(work),
            collection,
            [first],
            if_collection_exists="replace",
        )
        _write(
            domain_mongodb,
            PluginDestCtx(work),
            collection,
            [second],
            if_collection_exists="replace",
        )
        got = db.read_collection(collection)
        assert got["name"].to_list() == ["carol"]
    finally:
        db.drop_collection(collection)
