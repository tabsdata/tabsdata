#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain, mongodb_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.mongodb.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import MongoDBDest, _plugin as plugin

pytestmark = pytest.mark.mongodb


def _write_out(
    domain: MongodbDomain,
    work: Path,
    collection: str,
    parquet: Path,
    *,
    id_field: str | None = None,
    if_collection_exists: str = "append",
    docs_per_trx: int = 1000,
) -> None:
    plugin._MongoDBDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn(domain),
        MongoDBDest(
            collections_with_ids=[(collection, id_field)],
            if_collection_exists=if_collection_exists,
            docs_per_trx=docs_per_trx,
        ),
        [parquet],
    )


@fs_domain("fs_employees_xs")
@mongodb_domain("mongodb")
def test_dest_plugin_write_out_appends_documents(
    domain_fs_employees_xs: FsDomain,
    domain_mongodb: MongodbDomain,
    tmp_path: Path,
):
    db = store(domain_mongodb)
    collection = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    try:
        _write_out(
            domain_mongodb,
            work,
            collection,
            parquet,
            if_collection_exists="replace",
        )
        got = db.read_collection(collection)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)

        _write_out(domain_mongodb, work, collection, parquet)
        assert db.count(collection) == 2 * expected.height
    finally:
        db.drop_collection(collection)


@fs_domain("fs_employees_xs")
@mongodb_domain("mongodb")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_mongodb: MongodbDomain,
    tmp_path: Path,
):
    # The pre-existing documents are seeded with a field the entity does not have,
    # so the write must leave only its own documents and fields behind -- neither
    # the seeded documents nor their shape survive. They are seeded through the
    # store rather than by a second entity because every other employees entity
    # carries date columns, which the write path cannot BSON-encode.
    db = store(domain_mongodb)
    collection = "td_dest_replace"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    try:
        db.write_collection(collection, [{"stale": 1}, {"stale": 2}])
        _write_out(
            domain_mongodb, work, collection, parquet, if_collection_exists="replace"
        )
        got = db.read_collection(collection)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_collection(collection)


@fs_domain("fs_employees_xs")
@mongodb_domain("mongodb")
def test_dest_plugin_write_out_upserts_on_id_field(
    domain_fs_employees_xs: FsDomain,
    domain_mongodb: MongodbDomain,
    tmp_path: Path,
):
    # With an `id_field` the plugin copies that field into `_id` and upserts, so
    # re-loading the same rows updates the same documents instead of duplicating
    # them (which is what the append test asserts for an auto-generated `_id`).
    db = store(domain_mongodb)
    collection = "td_dest_upsert"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    try:
        _write_out(
            domain_mongodb,
            work,
            collection,
            parquet,
            id_field="dept_no",
            if_collection_exists="replace",
        )
        assert db.count(collection) == rows

        _write_out(domain_mongodb, work, collection, parquet, id_field="dept_no")
        assert db.count(collection) == rows
    finally:
        db.drop_collection(collection)


@fs_domain("fs_employees_xs")
@mongodb_domain("mongodb")
def test_dest_plugin_write_out_qualified_collection_targets_database(
    domain_fs_employees_xs: FsDomain,
    domain_mongodb: MongodbDomain,
    tmp_path: Path,
):
    # A bare collection resolves against the database named in the connection URI
    # (what every other test here relies on); a `<database>.<collection>` name
    # names it explicitly.
    db = store(domain_mongodb)
    collection = "td_dest_qualified"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    try:
        _write_out(
            domain_mongodb,
            work,
            f"{domain_mongodb.database}.{collection}",
            parquet,
            if_collection_exists="replace",
        )
        assert db.count(collection) == rows
    finally:
        db.drop_collection(collection)


@fs_domain("fs_employees_xs")
@mongodb_domain("mongodb")
def test_dest_plugin_write_out_chunks_by_docs_per_trx(
    domain_fs_employees_xs: FsDomain,
    domain_mongodb: MongodbDomain,
    tmp_path: Path,
):
    db = store(domain_mongodb)
    collection = "td_dest_chunked"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    assert expected.height > 2, "the entity must span several batches"
    try:
        _write_out(
            domain_mongodb,
            work,
            collection,
            parquet,
            if_collection_exists="replace",
            docs_per_trx=2,
        )
        assert db.read_collection(collection).height == expected.height
    finally:
        db.drop_collection(collection)
