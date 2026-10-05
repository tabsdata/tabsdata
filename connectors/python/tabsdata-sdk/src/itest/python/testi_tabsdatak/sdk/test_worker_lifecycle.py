#
# Copyright 2026 Tabsdata Inc.
#

"""Worker workloads on a cluster instance: what the bridge's reconciler does
to the jobs, cron jobs and pods behind functions when they are run,
cancelled, updated, disabled, enabled and deleted. The tests drive the
server through the SDK and read the cluster through the instance's
kubeconfig, finding each workload by its ``tabsdata.com/worker-id`` label.
"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    wait_for_completion,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from testi_tabsdatak.conn.common.cluster import (
    COLLECTION_ANNOTATION,
    FUNCTION_ANNOTATION,
    FUNCTION_ID,
    LABEL_VALUE_MAX_LENGTH,
    WORKER_ID,
    Cluster,
    eventually,
    label_value,
)
from testi_tabsdatak.sdk.workloads import (
    CANCELED,
    FAILED,
    FUNCTIONS,
    LONG_NAME,
    REMOVAL_TIMEOUT,
    STARTUP_TIMEOUT,
    cron_job_of,
    current_status,
    finish,
    lifecycle,
    register,
    run_worker,
    running_run_worker,
    source_collection,
)

pytestmark = [pytest.mark.sdk, pytest.mark.slow]


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_cancelling_an_execution_stops_its_running_workload(
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

        execution.cancel()

        eventually(
            lambda: not cluster.of_worker(worker.id),
            REMOVAL_TIMEOUT,
            "the cancelled run's workload was not removed",
        )
        assert current_status(execution, worker.id) == FAILED
        assert {run.status for run in execution.function_runs} == {CANCELED}
    finally:
        finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_cancelling_a_transaction_stops_its_running_workload(
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

        for transaction in execution.transactions:
            transaction.cancel()

        eventually(
            lambda: not cluster.of_worker(worker.id),
            REMOVAL_TIMEOUT,
            "the cancelled transaction's workload was not removed",
        )
        assert current_status(execution, worker.id) == FAILED
    finally:
        finish(execution)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_finished_runs_keep_their_workloads(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    function = register(source_collection(collection_factory), lifecycle.quick, "quick")

    first = function.trigger()
    wait_for_completion(first)
    first_worker = run_worker(first)
    second = function.trigger()
    wait_for_completion(second)
    second_worker = run_worker(second)

    assert first_worker is not None and second_worker is not None
    assert cluster.jobs(f"{WORKER_ID}={label_value(first_worker.id)}")
    assert cluster.jobs(f"{WORKER_ID}={label_value(second_worker.id)}")


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_names_are_annotations_and_labels_carry_only_ids(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.quick, LONG_NAME)

    execution = function.trigger()
    wait_for_completion(execution)
    worker = run_worker(execution)
    assert worker is not None
    selector = f"{WORKER_ID}={label_value(worker.id)}"
    objects = (
        cluster.jobs(selector)
        + cluster.configmaps(selector)
        + cluster.secrets(selector)
    )

    assert cluster.jobs(selector)
    assert len(cluster.configmaps(selector)) == 2
    assert len(cluster.secrets(selector)) == 1
    function_ids = {obj.metadata.labels[FUNCTION_ID] for obj in objects}
    assert len(function_ids) == 1
    for obj in objects:
        labels = obj.metadata.labels
        annotations = obj.metadata.annotations or {}
        assert FUNCTION_ANNOTATION not in labels
        assert COLLECTION_ANNOTATION not in labels
        assert all(len(value) <= LABEL_VALUE_MAX_LENGTH for value in labels.values())
        assert annotations[FUNCTION_ANNOTATION] == LONG_NAME
        assert annotations[COLLECTION_ANNOTATION] == collection.name


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_updating_a_cron_function_replaces_its_cron_job(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")
    try:
        old = cron_job_of(function, cluster)
        function_id = old.metadata.labels[FUNCTION_ID]
        old_worker = old.metadata.labels[WORKER_ID]

        collection.update_function(
            "ticker", info=lifecycle.ticker, project_root=str(FUNCTIONS)
        )

        def replaced():
            cronjobs = cluster.cronjobs_of_function(function_id)
            if (
                len(cronjobs) == 1
                and cronjobs[0].metadata.labels[WORKER_ID] != old_worker
            ):
                return cronjobs[0]
            return None

        new = eventually(
            replaced, STARTUP_TIMEOUT, "the updated cron trigger was not replaced"
        )
        assert new.metadata.labels[FUNCTION_ID] == function_id
    finally:
        collection.get_function("ticker").delete()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_disabling_and_enabling_a_cron_function_toggles_its_cron_job(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")
    try:
        function_id = cron_job_of(function, cluster).metadata.labels[FUNCTION_ID]

        function.enable(False)
        eventually(
            lambda: not cluster.cronjobs_of_function(function_id),
            REMOVAL_TIMEOUT,
            "the disabled cron trigger kept its cron job",
        )

        function.enable(True)
        cronjobs = eventually(
            lambda: cluster.cronjobs_of_function(function_id),
            STARTUP_TIMEOUT,
            "the re-enabled cron trigger got no cron job",
        )
        assert len(cronjobs) == 1
    finally:
        collection.get_function("ticker").delete()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_deleting_a_cron_function_removes_its_cron_job(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    cluster = Cluster(instance_tdtest)
    collection = source_collection(collection_factory)
    function = register(collection, lifecycle.ticker, "ticker")
    function_id = cron_job_of(function, cluster).metadata.labels[FUNCTION_ID]

    function.delete()

    eventually(
        lambda: not cluster.cronjobs_of_function(function_id),
        REMOVAL_TIMEOUT,
        "the deleted cron trigger kept its cron job",
    )
