#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB destination-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mongodb import MongodbDomain, mongodb_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.sql.function_project import function_module
from testi_tabsdatak.conn.mongodb.backend import dest_conn, store

from tabsdatak.conn.dev import DataGenSrcConn

pytestmark = pytest.mark.mongodb

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")
subscriber = function_module(FUNCTIONS, "subscriber")


@tabsdata_instance(consumer="tdtest")
@mongodb_domain("mongodb")
def test_subscriber_writes_collection_to_mongodb_domain(
    instance_tdtest: Instance,
    domain_mongodb: MongodbDomain,
    collection_factory: CollectionFactory,
) -> None:
    # MongoDB is destination-only, so the input table is filled by the dev data
    # generator -- its rows live in the publisher module, so no other connector and
    # no external system feed this test.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    db = store(domain_mongodb)
    source = collection_factory(
        "sources", DataGenSrcConn(), publisher.SOURCE_COLLECTION
    )
    run_function(
        source,
        publisher.pub_subscriber_writes_collection_to_mongodb_domain,
        FUNCTIONS,
    )

    destination = collection_factory("destinations", dest_conn(domain_mongodb))
    try:
        run_function(
            destination,
            subscriber.sub_subscriber_writes_collection_to_mongodb_domain,
            FUNCTIONS,
        )
        assert_frames_match(
            db.read_collection(subscriber.SUBSCRIBED_COLLECTION),
            publisher.GENERATED_FRAME,
        )
    finally:
        db.drop_collection(subscriber.SUBSCRIBED_COLLECTION)
