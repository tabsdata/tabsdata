#
# Copyright 2026 Tabsdata Inc.
#

"""Stage-trigger workloads on a cluster instance: the streamer job behind a
stream publisher across repeated enables, disables, updates and deletion,
while a `Monitor` checks that two streamers of the same function never
coexist. The publishers read empty topics, so their streamers idle until
stopped.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.cluster import (
    FUNCTION_ID,
    Cluster,
    Monitor,
    eventually,
    worker_parts,
    worker_selector,
)
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.kafka.backend import broker, src_conn

from tabsdatak.sdk.status_utils.worker import WorkerStatus, worker_status_to_mapping
from tabsdatak.sdk.tabsdata_server import Collection, Function, Worker

pytestmark = [pytest.mark.kafka, pytest.mark.slow]

FUNCTIONS = Path(__file__).parent / "functions"
streamer = function_module(FUNCTIONS, "streamer")

STAGE_TRIGGER = "ST"
RUNNING = worker_status_to_mapping(WorkerStatus.RUNNING.value)
STARTUP_TIMEOUT = 600
REMOVAL_TIMEOUT = 180
PAUSES = [0, 0, 1, 3, 0, 5, 0, 2]


def register(collection: Collection, info, name: str) -> Function:
    function = collection.register_function(
        info=info, project_root=str(FUNCTIONS), function_name=name
    )
    function.trigger()
    return function


def stage_workers(function: Function) -> list[Worker]:
    return [worker for worker in function.workers if worker.type == STAGE_TRIGGER]


def running_streamer_function_id(function: Function, cluster: Cluster) -> str:
    def found() -> str | None:
        for worker in stage_workers(function):
            if worker.status != RUNNING:
                continue
            if cluster.running_pod_of_worker(worker.id) is None:
                continue
            return cluster.of_worker(worker.id)[0].metadata.labels[FUNCTION_ID]
        return None

    return eventually(found, STARTUP_TIMEOUT, "the streamer never got a running pod")


def settled_on_newest(function: Function, cluster: Cluster, function_id: str) -> bool:
    newest = max((worker.id for worker in stage_workers(function)), key=worker_parts)
    unfinished = cluster.unfinished_of_function(function_id)
    if len(unfinished) != 1:
        return False
    jobs = cluster.jobs(worker_selector(newest))
    return any(job.metadata.name in unfinished for job in jobs)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(2400)
def test_toggling_a_stream_publisher_repeatedly_settles_on_the_last_state(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = collection_factory("sources", src_conn(domain_redpanda))
    with broker(domain_redpanda).fresh_topic(streamer.TOGGLE_TOPIC):
        function = register(collection, streamer.toggled, "toggled")
        try:
            function_id = running_streamer_function_id(function, cluster)
            with Monitor(cluster, function_id) as monitor:
                enabled = True
                for pause in PAUSES:
                    enabled = not enabled
                    function.enable(enabled)
                    time.sleep(pause)
                assert enabled
                eventually(
                    lambda: settled_on_newest(function, cluster, function_id),
                    STARTUP_TIMEOUT,
                    "the stream publisher did not settle on one streamer when enabled",
                )

                for pause in PAUSES[:5]:
                    enabled = not enabled
                    function.enable(enabled)
                    time.sleep(pause)
                assert not enabled
                eventually(
                    lambda: not cluster.unfinished_of_function(function_id),
                    REMOVAL_TIMEOUT,
                    "the stream publisher kept a streamer when left disabled",
                )
            assert not monitor.violations, monitor.violations[:5]
        finally:
            function.enable(False)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(2400)
def test_updating_a_stream_publisher_repeatedly_keeps_the_newest_streamer(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = collection_factory("sources", src_conn(domain_redpanda))
    with broker(domain_redpanda).fresh_topic(streamer.UPDATE_TOPIC):
        function = register(collection, streamer.updated, "updated")
        try:
            function_id = running_streamer_function_id(function, cluster)
            with Monitor(cluster, function_id) as monitor:
                for pause in [0, 0, 2, 0]:
                    collection.update_function(
                        "updated", info=streamer.updated, project_root=str(FUNCTIONS)
                    )
                    time.sleep(pause)
                eventually(
                    lambda: settled_on_newest(function, cluster, function_id),
                    STARTUP_TIMEOUT,
                    "the stream publisher did not settle on its newest streamer",
                )
            assert not monitor.violations, monitor.violations[:5]
        finally:
            collection.get_function("updated").enable(False)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(1800)
def test_deleting_a_stream_publisher_removes_its_streamer(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = collection_factory("sources", src_conn(domain_redpanda))
    with broker(domain_redpanda).fresh_topic(streamer.DELETE_TOPIC):
        function = register(collection, streamer.deleted, "deleted")
        function_id = running_streamer_function_id(function, cluster)

        function.delete()

        eventually(
            lambda: not cluster.unfinished_of_function(function_id),
            REMOVAL_TIMEOUT,
            "the deleted stream publisher kept its streamer",
        )


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(2400)
def test_mixed_operations_on_a_stream_publisher_settle_on_the_last_request(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = collection_factory("sources", src_conn(domain_redpanda))

    def update():
        collection.update_function(
            "mixed", info=streamer.mixed, project_root=str(FUNCTIONS)
        )

    with broker(domain_redpanda).fresh_topic(streamer.MIXED_TOPIC):
        function = register(collection, streamer.mixed, "mixed")
        try:
            function_id = running_streamer_function_id(function, cluster)
            with Monitor(cluster, function_id) as monitor:
                for operation in [
                    update,
                    lambda: function.enable(False),
                    lambda: function.enable(True),
                    update,
                    lambda: function.enable(False),
                    update,
                    lambda: function.enable(True),
                    update,
                ]:
                    operation()
                eventually(
                    lambda: settled_on_newest(function, cluster, function_id),
                    STARTUP_TIMEOUT,
                    "the stream publisher did not settle on the last request",
                )
            assert not monitor.violations, monitor.violations[:5]
        finally:
            collection.get_function("mixed").enable(False)
