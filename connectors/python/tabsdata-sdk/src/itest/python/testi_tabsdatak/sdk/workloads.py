#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

from tabsdatax.itest.fixture.functions.harness import CollectionFactory

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.sdk.status_utils.execution import EXECUTION_FINAL_STATUSES
from tabsdatak.sdk.status_utils.function_run import (
    FunctionRunStatus,
    function_run_status_to_mapping,
)
from tabsdatak.sdk.status_utils.worker import WorkerStatus, worker_status_to_mapping
from tabsdatak.sdk.tabsdata_server import Collection, Execution, Function, Worker
from testi_tabsdatak.conn.common.cluster import (
    FUNCTION_ID,
    WORKER_ID,
    Cluster,
    eventually,
    label_value,
)
from testi_tabsdatak.conn.common.function_project import function_module

FUNCTIONS = Path(__file__).parent / "functions"
lifecycle = function_module(FUNCTIONS, "lifecycle")

FUNCTION_RUN = "F"
CRON_TRIGGER = "CT"

RUNNING = worker_status_to_mapping(WorkerStatus.RUNNING.value)
FAILED = worker_status_to_mapping(WorkerStatus.FAILED.value)
CANCELED = function_run_status_to_mapping(FunctionRunStatus.CANCELED.value)

STARTUP_TIMEOUT = 600
REMOVAL_TIMEOUT = 180

LONG_NAME = "long_" + "n" * 95


def source_collection(factory: CollectionFactory) -> Collection:
    return factory("sources", DataGenSrcConn())


def register(collection: Collection, info, name: str) -> Function:
    return collection.register_function(
        info=info, project_root=str(FUNCTIONS), function_name=name
    )


def run_worker(execution: Execution) -> Worker | None:
    return next(
        (worker for worker in execution.workers if worker.type == FUNCTION_RUN),
        None,
    )


def running_run_worker(execution: Execution, cluster: Cluster) -> Worker:
    def ready() -> Worker | None:
        worker = run_worker(execution)
        if (
            worker is not None
            and worker.status == RUNNING
            and cluster.unfinished_job_of_worker(worker.id) is not None
        ):
            return worker
        return None

    return eventually(ready, STARTUP_TIMEOUT, "the run never got an unfinished job")


def current_status(execution: Execution, worker_id: str) -> str:
    return next(worker.status for worker in execution.workers if worker.id == worker_id)


def finish(execution: Execution) -> None:
    execution.refresh()
    if execution.status not in EXECUTION_FINAL_STATUSES:
        execution.cancel()


def cron_job_of(function: Function, cluster: Cluster):
    def found():
        for worker in function.workers:
            if worker.type != CRON_TRIGGER:
                continue
            cronjobs = cluster.cronjobs(f"{WORKER_ID}={label_value(worker.id)}")
            if cronjobs:
                return cronjobs[0]
        return None

    return eventually(found, STARTUP_TIMEOUT, "the cron trigger never got a cron job")


def function_id_of(cluster: Cluster, worker_id: str) -> str:
    return cluster.of_worker(worker_id)[0].metadata.labels[FUNCTION_ID]


def cron_workers(function: Function) -> list[Worker]:
    return [worker for worker in function.workers if worker.type == CRON_TRIGGER]
