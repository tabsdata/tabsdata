#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL CDC initial-load integration tests using tabsdata.

The fake-context suite in test_plugin_cdc_initial_load.py drives the plugin
directly; these run the same feature through a real instance, so the execution
plan, the staged manifests and the published table are the server's own.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mysqlcdc import MysqlcdcDomain, mysqlcdc_domain
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.common.sql.cdc_instance import (
    DELETE,
    INSERT,
    LOAD,
    TRUNCATE,
    UPDATE,
    await_published,
    captured_ids,
    loaded_ids,
    ops,
    start_streamer,
    stop_streamer,
)
from testi_tabsdatak.conn.mysql.backend import cdc_probe, cdc_src_conn

# Every mysql CDC test shares the domain's binlog, so they all name one xdist
# group and run one after another on a single worker.
pytestmark = [
    pytest.mark.mysql,
    pytest.mark.xdist_group("mysqlcdc_seq"),
]

FUNCTIONS = Path(__file__).parent / "functions"
cdc_publisher = function_module(FUNCTIONS, "cdc_publisher")

KEY = "id"
FUNCTION = cdc_publisher.pub_stream_publisher_initial_load_from_mysqlcdc_domain
SEEDED = [row["id"] for row in cdc_publisher.SEEDED_ROWS]


@tabsdata_instance(consumer="tdtest")
@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_stream_publisher_initial_load_from_mysqlcdc_domain(
    instance_tdtest: Instance,
    domain_mysqlcdc_employees_xs: MysqlcdcDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, cdc_publisher.TRACKED_TABLE) as probe:
        for row in cdc_publisher.SEEDED_ROWS:
            probe.insert(row["id"], row["name"])

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

            probe.insert(101, "after-the-load")
            probe.update(1, "changed-after-the-load")
            probe.delete(2)

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
@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_stream_publisher_restart_does_not_reload_on_mysqlcdc_domain(
    instance_tdtest: Instance,
    domain_mysqlcdc_employees_xs: MysqlcdcDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, cdc_publisher.TRACKED_TABLE) as probe:
        for row in cdc_publisher.SEEDED_ROWS:
            probe.insert(row["id"], row["name"])

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
            probe.insert(202, "written-while-stopped")
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


@tabsdata_instance(consumer="tdtest")
@mysqlcdc_domain("mysqlcdc_employees_xs")
def test_stream_publisher_truncate_after_the_load_on_mysqlcdc_domain(
    instance_tdtest: Instance,
    domain_mysqlcdc_employees_xs: MysqlcdcDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_mysqlcdc_employees_xs
    with cdc_probe(domain, cdc_publisher.TRACKED_TABLE) as probe:
        for row in cdc_publisher.SEEDED_ROWS:
            probe.insert(row["id"], row["name"])

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

            probe.truncate()
            truncated = await_published(
                collection,
                cdc_publisher.PUBLISHED_TABLE,
                lambda frame: TRUNCATE in ops(frame),
            )
        finally:
            function.enable(False)

    assert TRUNCATE in ops(truncated)
    assert loaded_ids(truncated, KEY) == []
