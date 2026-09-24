#
# Copyright 2026 Tabsdata Inc.
#

"""Db2 CDC initial-load integration tests using tabsdata.

The fake-context suite in test_plugin_cdc_initial_load.py drives the plugin
directly; these run the same feature through a real instance, so the execution
plan, the staged manifests and the published table are the server's own.

There is no truncate test: IBM SQL Replication does not honour ``truncate
immediate``, so a destination reset is expressed as ``delete from`` and arrives
as regular delete events.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.db2cdc import Db2CdcDomain, db2cdc_domain
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.common.sql.cdc_instance import (
    DELETE,
    INSERT,
    LOAD,
    UPDATE,
    await_published,
    captured_ids,
    loaded_ids,
    ops,
    start_streamer,
    stop_streamer,
)
from testi_tabsdatak.conn.db2.backend import (
    cd_table_of,
    cdc_delete,
    cdc_insert,
    cdc_source_table,
    cdc_src_conn,
    cdc_update,
)

# Every db2 CDC test shares the domain's capture program, so they all name one
# xdist group and run one after another on a single worker.
pytestmark = [
    pytest.mark.db2,
    pytest.mark.db2_seq,
    pytest.mark.xdist_group("db2cdc_seq"),
]

FUNCTIONS = Path(__file__).parent / "functions"
cdc_publisher = function_module(FUNCTIONS, "cdc_publisher")

KEY = "ID"
FUNCTION = cdc_publisher.pub_stream_publisher_initial_load_from_db2cdc_domain
SEEDED = [row["id"] for row in cdc_publisher.SEEDED_ROWS]


def test_the_checked_in_cd_table_matches_what_the_domain_derives():
    assert cd_table_of(cdc_publisher.SOURCE_TABLE) == cdc_publisher.TRACKED_TABLE


@tabsdata_instance(consumer="tdtest")
@db2cdc_domain("db2cdc_employees_xs")
def test_stream_publisher_initial_load_from_db2cdc_domain(
    instance_tdtest: Instance,
    domain_db2cdc_employees_xs: Db2CdcDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain, cdc_publisher.SOURCE_TABLE) as (table, _cd_table):
        cdc_insert(domain, table, cdc_publisher.SEEDED_ROWS)

        collection = collection_factory("sources", cdc_src_conn(domain))
        function = collection.register_function(
            info=FUNCTION, project_root=str(FUNCTIONS)
        )
        try:
            function.trigger()
            loaded = await_published(
                collection,
                cdc_publisher.PUBLISHED_TABLE,
                lambda frame: loaded_ids(frame, KEY) == SEEDED,
            )

            cdc_insert(domain, table, [{"id": 101, "name": "after", "age": 101}])
            cdc_update(domain, table, 1, "changed-after-the-load", 11)
            cdc_delete(domain, table, 2)

            captured = await_published(
                collection,
                cdc_publisher.PUBLISHED_TABLE,
                lambda frame: (
                    {UPDATE, DELETE} <= set(ops(frame))
                    and 101 in captured_ids(frame, KEY)
                ),
            )
        finally:
            # The streamer runs until disabled; leaving it up leaks a pod past
            # teardown.
            function.enable(False)

    # The harness reads the table at HEAD, so the load and the changes after it
    # are asserted on the batch each one produced.
    assert loaded_ids(loaded, KEY) == SEEDED
    assert ops(loaded).count(LOAD) == 1
    assert ops(loaded).count(INSERT) == len(SEEDED)

    assert 101 in captured_ids(captured, KEY)
    assert {INSERT, UPDATE, DELETE} <= set(ops(captured))


@tabsdata_instance(consumer="tdtest")
@db2cdc_domain("db2cdc_employees_xs")
def test_stream_publisher_restart_does_not_reload_on_db2cdc_domain(
    instance_tdtest: Instance,
    domain_db2cdc_employees_xs: Db2CdcDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_db2cdc_employees_xs
    with cdc_source_table(domain, cdc_publisher.SOURCE_TABLE) as (table, _cd_table):
        cdc_insert(domain, table, cdc_publisher.SEEDED_ROWS)

        collection = collection_factory("sources", cdc_src_conn(domain))
        function = collection.register_function(
            info=FUNCTION, project_root=str(FUNCTIONS)
        )
        try:
            function.trigger()
            await_published(
                collection,
                cdc_publisher.PUBLISHED_TABLE,
                lambda frame: loaded_ids(frame, KEY) == SEEDED,
            )

            stop_streamer(function)
            cdc_insert(
                domain, table, [{"id": 202, "name": "while-stopped", "age": 202}]
            )
            start_streamer(function)

            resumed = await_published(
                collection,
                cdc_publisher.PUBLISHED_TABLE,
                lambda frame: 202 in captured_ids(frame, KEY),
            )
        finally:
            function.enable(False)

    assert 202 in captured_ids(resumed, KEY)
    assert loaded_ids(resumed, KEY) == []
    assert LOAD not in ops(resumed)
