#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict
from typing import Callable, TypeVar

from kubernetes import client, config
from tabsdatax.itest.fixture.instances.tabsdata import Instance

WORKER_ID = "tabsdata.com/worker-id"
FUNCTION_ID = "tabsdata.com/function-id"
DECORATOR = "tabsdata.com/decorator"
FUNCTION_ANNOTATION = "tabsdata.com/function"
COLLECTION_ANNOTATION = "tabsdata.com/collection"
LABEL_VALUE_MAX_LENGTH = 63

T = TypeVar("T")


def label_value(value: str) -> str:
    mapped = re.sub(r"[^A-Za-z0-9._-]", "-", value)[:LABEL_VALUE_MAX_LENGTH]
    return re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", mapped)


def eventually(
    condition: Callable[[], T | None],
    timeout: float,
    message: str,
    interval: float = 2.0,
) -> T:
    deadline = time.monotonic() + timeout
    while True:
        outcome = condition()
        if outcome:
            return outcome
        if time.monotonic() >= deadline:
            raise AssertionError(f"{message} (after {timeout:.0f}s)")
        time.sleep(interval)


def finished(job) -> bool:
    status = job.status
    return bool(status and (status.succeeded or status.failed))


def owned_by_cronjob(job) -> bool:
    return any(owner.kind == "CronJob" for owner in job.metadata.owner_references or [])


class Cluster:
    def __init__(self, instance: Instance):
        resolved = instance.settings["cluster"]["resolved"]
        api = config.new_client_from_config(
            config_file=resolved["kubeconfig"], context=resolved.get("context")
        )
        self.namespace = resolved.get("namespace") or "default"
        self.core = client.CoreV1Api(api)
        self.batch = client.BatchV1Api(api)

    def jobs(self, selector: str) -> list:
        return self.batch.list_namespaced_job(
            self.namespace, label_selector=selector
        ).items

    def cronjobs(self, selector: str) -> list:
        return self.batch.list_namespaced_cron_job(
            self.namespace, label_selector=selector
        ).items

    def pods(self, selector: str) -> list:
        return self.core.list_namespaced_pod(
            self.namespace, label_selector=selector
        ).items

    def configmaps(self, selector: str) -> list:
        return self.core.list_namespaced_config_map(
            self.namespace, label_selector=selector
        ).items

    def secrets(self, selector: str) -> list:
        return self.core.list_namespaced_secret(
            self.namespace, label_selector=selector
        ).items

    def of_worker(self, worker_id: str) -> list:
        selector = f"{WORKER_ID}={label_value(worker_id)}"
        return self.jobs(selector) + self.cronjobs(selector) + self.pods(selector)

    def running_pod_of_worker(self, worker_id: str):
        selector = f"{WORKER_ID}={label_value(worker_id)}"
        return next(
            (pod for pod in self.pods(selector) if pod.status.phase == "Running"),
            None,
        )

    def cronjobs_of_function(self, function_id: str) -> list:
        return self.cronjobs(f"{FUNCTION_ID}={label_value(function_id)}")

    def unfinished_by_kind(self, function_id: str) -> dict[str, set[str]]:
        selector = f"{FUNCTION_ID}={label_value(function_id)}"
        groups: dict[str, set[str]] = defaultdict(set)
        for job in self.jobs(selector):
            if owned_by_cronjob(job) or finished(job):
                continue
            groups[job.metadata.labels.get(DECORATOR, "")].add(job.metadata.name)
        for cronjob in self.cronjobs(selector):
            groups[cronjob.metadata.labels.get(DECORATOR, "")].add(
                cronjob.metadata.name
            )
        return groups

    def unfinished_of_function(self, function_id: str) -> set[str]:
        return {
            name
            for names in self.unfinished_by_kind(function_id).values()
            for name in names
        }

    def create_suspended_job(
        self, name: str, labels: dict, annotations: dict, image: str
    ) -> None:
        metadata = client.V1ObjectMeta(
            name=name, labels=labels, annotations=annotations
        )
        container = client.V1Container(name="orphan", image=image, command=["true"])
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container]),
        )
        body = client.V1Job(
            metadata=metadata, spec=client.V1JobSpec(suspend=True, template=template)
        )
        self.batch.create_namespaced_job(self.namespace, body)

    def delete_job(self, name: str) -> None:
        self.batch.delete_namespaced_job(
            name, self.namespace, propagation_policy="Foreground"
        )


class Monitor:
    def __init__(self, cluster: Cluster, function_id: str, interval: float = 1.0):
        self.cluster = cluster
        self.function_id = function_id
        self.interval = interval
        self.violations: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self) -> Monitor:
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        self._thread.join()

    def _sample(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                groups = self.cluster.unfinished_by_kind(self.function_id)
            except client.ApiException:
                continue
            for decorator, names in groups.items():
                if len(names) > 1:
                    self.violations.append(f"{decorator}: {sorted(names)}")
