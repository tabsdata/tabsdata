#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end MongoDB connector tests: register real functions on a running
tabsdata instance, trigger them, and assert the outcome against a tdx-provisioned
``mongodb`` domain -- unlike ``test__plugin_integration.py``, which drives the
plugin directly with a fabricated context.

MongoDB is destination-only (no ``MongoDBSrc``), so the source table is seeded by
the ``dev`` connector's ``DataGenSrc`` (in-line generated rows, no external
backend) rather than by the connector's own source. The generic harness and the
credential/parquet helpers are shared under
:mod:`tabsdatax.itest.fixture.functions`; only the mongodb ``Conn`` construction
and the document verification are here. Requires the ``tdtest`` instance up and
the ``mongodb`` domain provisioned.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain, mongodb_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import sql_credentials, write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.conn.mongodb import MongoDBDestConn

pytestmark = pytest.mark.mongodb

_TABLE = "e2e_mongodb_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _dest_conn(domain: MongodbDomain) -> MongoDBDestConn:
    return MongoDBDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def _seed_source(collection_factory: CollectionFactory, project_root: Path) -> object:
    """Publish in-line ``DataGenSrc`` rows into a fresh source collection (MongoDB
    has no Src of its own); return the collection."""
    info = write_sql_publisher(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"DataGenSrc(data=[Data(pl.DataFrame({_DATA!r}))])",
        output_table=_TABLE,
    )
    collection = collection_factory("sources", DataGenSrcConn())
    run_function(collection, info, project_root)
    return collection


@tabsdata_instance(consumer="tdtest")
@mongodb_domain("mongodb")
def test_subscriber_writes_collection_to_mongodb_domain(
    instance_tdtest: Instance,
    domain_mongodb: MongodbDomain,
    collection_factory: CollectionFactory,
    mongo,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # mongodb collection and assert the documents match. OS/kminus does not
    # auto-trigger subscribers, so trigger it directly.
    db = mongo(domain_mongodb)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    out_collection = f"e2e_out_{uuid.uuid4().hex[:8]}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.mongodb import MongoDBDest",
        dest_expr=(
            f"MongoDBDest(collections_with_ids=[({out_collection!r}, None)], "
            "if_collection_exists='replace')"
        ),
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_mongodb)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_collection(out_collection), expected)
    finally:
        db.drop_collection(out_collection)
