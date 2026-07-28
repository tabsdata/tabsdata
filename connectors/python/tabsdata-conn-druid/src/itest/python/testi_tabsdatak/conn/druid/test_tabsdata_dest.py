#
# Copyright 2026 Tabsdata Inc.
#

"""Druid destination-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.sql.function_project import function_module
from testi_tabsdatak.conn.druid.backend import dest_conn, store

from tabsdatak.conn.dev import DataGenSrcConn

# Grouped with the rest of the package: the shared cluster ingests a couple of
# tasks at a time, so these cannot run beside each other. See test_explorer.py.
pytestmark = [pytest.mark.druid, pytest.mark.xdist_group("druid_cluster_seq")]

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")
subscriber = function_module(FUNCTIONS, "subscriber")


@tabsdata_instance(consumer="tdtest")
@druid_domain("druid")
@s3_domain("s3")
def test_subscriber_writes_table_to_druid_domain(
    instance_tdtest: Instance,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    collection_factory: CollectionFactory,
) -> None:
    # The subscriber reads a tabsdata table, so the input table is filled by the
    # dev data generator -- its rows live in the publisher module, so no other
    # connector and no external system feed this test.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    db = store(domain_druid)
    expected = publisher.GENERATED_FRAME
    source = collection_factory(
        "sources", DataGenSrcConn(), publisher.SOURCE_COLLECTION
    )
    run_function(
        source, publisher.pub_subscriber_writes_table_to_druid_domain, FUNCTIONS
    )

    destination = collection_factory("destinations", dest_conn(domain_druid, domain_s3))
    try:
        run_function(
            destination,
            subscriber.sub_subscriber_writes_table_to_druid_domain,
            FUNCTIONS,
        )
        assert_frames_match(
            db.read_table(subscriber.SUBSCRIBED_TABLE, expected.height), expected
        )
    finally:
        db.drop_table(subscriber.SUBSCRIBED_TABLE)
