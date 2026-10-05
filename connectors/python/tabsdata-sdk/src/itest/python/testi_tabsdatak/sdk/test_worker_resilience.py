#
# Copyright 2026 Tabsdata Inc.
#

"""Resilience of the workload reconciler and of recovery on a cluster
instance. Each scenario hammers one function with interleaved requests while
a `Monitor` samples the cluster, and fails on any moment where two unfinished
workloads of the same function and kind coexist; the end state is then
checked against the last request. The recovery scenarios break the cluster
behind the server's back and expect recovery to repair it through the queue.
"""

from __future__ import annotations

import time

import pytest
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.sdk.tabsdata_server import Execution
from testi_tabsdatak.conn.common.cluster import (
    FUNCTION_ID,
    WORKER_ID,
    Cluster,
    Monitor,
    eventually,
    worker_parts,
    worker_selector,
)
from testi_tabsdatak.sdk.workloads import (
    FAILED,
    FUNCTIONS,
    REMOVAL_TIMEOUT,
    RUNNING,
    STARTUP_TIMEOUT,
    cron_job_of,
    cron_workers,
    current_status,
    finish,
    function_id_of,
    lifecycle,
    register,
    running_run_worker,
    source_collection,
)

pytestmark = [pytest.mark.sdk, pytest.mark.slow]

RECOVERY_TIMEOUT = 900
PAUSES = [0, 0, 1, 3, 0, 5, 0, 2]


def settle(cluster: Cluster, function_id: str, expected: int, timeout: float):
    return eventually(
        lambda: len(cluster.unfinished_of_function(function_id)) == expected,
        timeout,
        f"expected {expected} unfinished workload(s) for the function",
    )


def assert_no_overlap(monitor: Monitor):
    assert not monitor.violations, monitor.violations[:5]


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1800)
def test_two_executions_never_run_two_workloads_at_once(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    first = function.trigger()
    executions = [first]
    try:
        worker = running_run_worker(first, cluster)
        function_id = function_id_of(cluster, worker.id)
        with Monitor(cluster, function_id) as monitor:
            executions.append(function.trigger())
            time.sleep(60)
            for execution in executions:
                execution.cancel()
            settle(cluster, function_id, 0, REMOVAL_TIMEOUT)
        assert_no_overlap(monitor)
    finally:
        for execution in executions:
            finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1800)
def test_cancelling_right_after_triggering_leaves_no_workload(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    executions: list[Execution] = []
    try:
        for _ in range(5):
            execution = function.trigger()
            executions.append(execution)
            execution.cancel()

        def all_gone() -> bool:
            workers = [
                worker for execution in executions for worker in execution.workers
            ]
            return all(not cluster.of_worker(worker.id) for worker in workers)

        eventually(
            all_gone,
            STARTUP_TIMEOUT,
            "a run cancelled right after its trigger left a workload behind",
        )
        time.sleep(90)
        assert all_gone(), "a cancelled run's workload appeared late"
    finally:
        for execution in executions:
            finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(3600)
def test_cancelling_and_retriggering_in_a_row_ends_with_one_workload(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    first = function.trigger()
    executions = [first]
    try:
        function_id = function_id_of(cluster, running_run_worker(first, cluster).id)
        with Monitor(cluster, function_id) as monitor:
            first.cancel()
            for pause in PAUSES:
                execution = function.trigger()
                executions.append(execution)
                time.sleep(pause)
                execution.cancel()
            last = function.trigger()
            executions.append(last)
            worker = running_run_worker(last, cluster)
            settle(cluster, function_id, 1, REMOVAL_TIMEOUT)
            assert cluster.of_worker(worker.id)

            last.cancel()
            settle(cluster, function_id, 0, REMOVAL_TIMEOUT)
        assert_no_overlap(monitor)
    finally:
        for execution in executions:
            finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_toggling_a_cron_function_repeatedly_settles_on_the_last_state(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")
    try:
        function_id = cron_job_of(function, cluster).metadata.labels[FUNCTION_ID]
        with Monitor(cluster, function_id) as monitor:
            enabled = True
            for pause in PAUSES:
                enabled = not enabled
                function.enable(enabled)
                time.sleep(pause)
            assert enabled
            eventually(
                lambda: len(cluster.cronjobs_of_function(function_id)) == 1,
                STARTUP_TIMEOUT,
                "the cron function did not settle on one cron job when enabled",
            )

            for pause in PAUSES[:5]:
                enabled = not enabled
                function.enable(enabled)
                time.sleep(pause)
            assert not enabled
            eventually(
                lambda: not cluster.cronjobs_of_function(function_id),
                REMOVAL_TIMEOUT,
                "the cron function kept a cron job when left disabled",
            )
        assert_no_overlap(monitor)
    finally:
        collection.get_function("ticker").delete()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_updating_a_cron_function_repeatedly_keeps_the_newest_cron_job(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")
    try:
        function_id = cron_job_of(function, cluster).metadata.labels[FUNCTION_ID]
        with Monitor(cluster, function_id) as monitor:
            for pause in [0, 0, 2, 0]:
                collection.update_function(
                    "ticker", info=lifecycle.ticker, project_root=str(FUNCTIONS)
                )
                time.sleep(pause)
            newest = max(
                (worker.id for worker in cron_workers(function)), key=worker_parts
            )

            def settled_on_newest():
                cronjobs = cluster.cronjobs_of_function(function_id)
                return (
                    len(cronjobs) == 1
                    and cronjobs[0].metadata.labels[WORKER_ID] == newest
                )

            eventually(
                settled_on_newest,
                STARTUP_TIMEOUT,
                "the cron function did not settle on its newest version's cron job",
            )
        assert_no_overlap(monitor)
    finally:
        collection.get_function("ticker").delete()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_mixed_operations_on_a_cron_function_settle_on_the_last_request(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")

    def update():
        collection.update_function(
            "ticker", info=lifecycle.ticker, project_root=str(FUNCTIONS)
        )

    try:
        function_id = cron_job_of(function, cluster).metadata.labels[FUNCTION_ID]
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
            newest = max(
                (worker.id for worker in cron_workers(function)), key=worker_parts
            )

            def settled_on_newest():
                cronjobs = cluster.cronjobs_of_function(function_id)
                return (
                    len(cronjobs) == 1
                    and cronjobs[0].metadata.labels[WORKER_ID] == newest
                )

            eventually(
                settled_on_newest,
                STARTUP_TIMEOUT,
                "the cron function did not settle on the last request",
            )
        assert_no_overlap(monitor)
    finally:
        collection.get_function("ticker").delete()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1800)
def test_deleting_a_function_right_after_cancelling_leaves_nothing(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    execution = function.trigger()
    try:
        worker = running_run_worker(execution, cluster)
        function_id = function_id_of(cluster, worker.id)

        execution.cancel()
        function.delete()

        settle(cluster, function_id, 0, REMOVAL_TIMEOUT)
        assert not cluster.of_worker(worker.id)
    finally:
        finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_a_lost_workload_is_recovered_with_a_new_worker(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    execution = function.trigger()
    try:
        lost = running_run_worker(execution, cluster)
        function_id = function_id_of(cluster, lost.id)
        job = cluster.jobs(worker_selector(lost.id))[0]

        with Monitor(cluster, function_id) as monitor:
            cluster.delete_job(job.metadata.name)

            def replacement():
                return next(
                    (
                        candidate
                        for candidate in execution.workers
                        if candidate.id != lost.id
                        and candidate.status == RUNNING
                        and cluster.unfinished_job_of_worker(candidate.id) is not None
                    ),
                    None,
                )

            eventually(
                replacement,
                RECOVERY_TIMEOUT,
                "recovery did not dispatch the run again after its workload was lost",
            )
            assert current_status(execution, lost.id) == FAILED
        assert_no_overlap(monitor)
    finally:
        finish(execution)


ORPHAN_COUNTER_OFFSET = 1_000_000


def orphan_of(cluster: Cluster, worker_id: str) -> tuple[str, dict, dict, str]:
    job = cluster.jobs(worker_selector(worker_id))[0]
    day, counter = worker_parts(worker_id)
    orphan_id = f"{day}-{counter + ORPHAN_COUNTER_OFFSET}"
    prefix = job.metadata.name.removesuffix(f"-{worker_id}")
    labels = dict(job.metadata.labels)
    labels[WORKER_ID] = orphan_id
    for managed in ["controller-uid", "batch.kubernetes.io/controller-uid", "job-name"]:
        labels.pop(managed, None)
        labels.pop(f"batch.kubernetes.io/{managed}", None)
    image = job.spec.template.spec.containers[0].image
    return f"{prefix}-{orphan_id}", labels, dict(job.metadata.annotations or {}), image


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_an_orphan_workload_is_removed_and_the_real_one_kept(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    execution = function.trigger()
    try:
        worker = running_run_worker(execution, cluster)
        orphan, labels, annotations, image = orphan_of(cluster, worker.id)

        cluster.create_suspended_job(orphan, labels, annotations, image)

        eventually(
            lambda: not cluster.jobs(f"{WORKER_ID}={labels[WORKER_ID]}"),
            RECOVERY_TIMEOUT,
            "recovery did not remove the orphan workload",
        )
        assert cluster.unfinished_job_of_worker(worker.id) is not None
    finally:
        finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(2400)
def test_an_orphan_without_a_function_id_is_left_in_place(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(
        source_collection(collection_factory), lifecycle.sleeper, "sleeper"
    )
    execution = function.trigger()
    orphan = None
    try:
        worker = running_run_worker(execution, cluster)
        orphan, labels, annotations, image = orphan_of(cluster, worker.id)
        labels.pop(FUNCTION_ID)

        cluster.create_suspended_job(orphan, labels, annotations, image)
        time.sleep(RECOVERY_TIMEOUT / 2)

        assert cluster.jobs(f"{WORKER_ID}={labels[WORKER_ID]}"), (
            "an inconsistent orphan must be reported, not stopped"
        )
        assert cluster.unfinished_job_of_worker(worker.id) is not None
    finally:
        if orphan is not None:
            cluster.delete_job(orphan)
        finish(execution)
