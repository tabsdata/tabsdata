#
# Copyright 2026 Tabsdata Inc.
#

"""The monkey engine without a server: plans, descriptors, the model, and whole
runs against an in-memory fake of the server and the cluster, so a broken
check shows up here rather than an hour into a cluster run."""

from __future__ import annotations

import dataclasses
import itertools
import threading
import time
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from tabsdatak.sdk.status_utils.execution import EXECUTION_FINAL_STATUSES
from testi_tabsdatak.conn.common.cluster import (
    DECORATOR,
    FUNCTION_ID,
    FUNCTION_VERSION_ID,
)
from testi_tabsdatak.conn.common.monkey import runner
from testi_tabsdatak.conn.common.monkey.descriptor import load_plan, write_plan
from testi_tabsdatak.conn.common.monkey.model import ANY, ERROR, OK, REJECTED, Model
from testi_tabsdatak.conn.common.monkey.naming import collection_name
from testi_tabsdatak.conn.common.monkey.plan import (
    CRON_PUBLISHER,
    DELETE,
    DISABLE,
    ENABLE,
    FAULTS,
    REGISTER,
    STREAM_PUBLISHER,
    TRIGGER,
    Op,
    Plan,
    generate,
)

NAMESPACE = "mktest"
PROJECT = "e2e_monkey_proj"
PROJECT_ID = "06ggproject0000000000000000"
FINAL = sorted(EXECUTION_FINAL_STATUSES)[0]


def rejected(status: int):
    response = requests.Response()
    response.status_code = status
    try:
        raise requests.HTTPError(response=response)
    except requests.HTTPError:
        raise RuntimeError(f"rejected with {status}") from None


class FakeExecution:
    ids = itertools.count()

    def __init__(self, function: str = "pub00", status: str = FINAL):
        self.id = f"exe{next(self.ids)}"
        self.status = status
        self.workers = [
            SimpleNamespace(
                id="wkr20261005-1", type="F", status="Done", function=function
            )
        ]
        self.transactions = [
            SimpleNamespace(
                id=f"tx{next(self.ids)}",
                status="Running",
                function_runs=[
                    SimpleNamespace(function=function, status="Done"),
                    SimpleNamespace(function="tfr00", status="Canceled"),
                ],
                cancel=lambda: None,
                recover=lambda: None,
            )
        ]

    def cancel(self):
        pass

    def recover(self):
        pass

    def refresh(self):
        pass


class FakeFunction:
    versions = itertools.count(1)
    ids = itertools.count(1)

    def __init__(self, server: FakeServer, collection: str, name: str):
        self.server = server
        self.collection = collection
        self.name = name
        self.enabled = True
        self.id = f"fv20261005-{next(self.versions)}"
        self.kwargs = {"function_id": f"fn{next(self.ids):024d}"}

    def delete(self):
        with self.server.lock:
            self.server.collections[self.collection].pop(self.name)

    def enable(self, enabled: bool = True):
        with self.server.lock:
            if self.server.drift and not enabled:
                return
            self.enabled = enabled
            self.id = f"fv20261005-{next(self.versions)}"

    def trigger(self):
        if not self.enabled:
            rejected(400)
        return FakeExecution(self.name, "Running" if self.server.stuck else FINAL)


class FakeCollection:
    def __init__(self, server: FakeServer, name: str):
        self.server = server
        self.name = name

    def register_function(self, info, project_root, function_name, reuse_tables):
        with self.server.lock:
            functions = self.server.collections[self.name]
            if function_name in functions:
                rejected(409)
            function = FakeFunction(self.server, self.name, function_name)
            functions[function_name] = function

    def update_function(self, function_name, info, project_root, enabled):
        function = self.get_function(function_name)
        with self.server.lock:
            function.enabled = enabled
            function.id = f"fv20261005-{next(FakeFunction.versions)}"

    def get_function(self, function_name):
        with self.server.lock:
            function = self.server.collections[self.name].get(function_name)
        if function is None:
            rejected(404)
        return function


class FakeServer:
    def __init__(self, plan: Plan, drift: bool = False, stuck: bool = False):
        self.lock = threading.Lock()
        self.drift = drift
        self.stuck = stuck
        self.transactions: list = []
        self.runs: list = []
        self.collections: dict[str, dict[str, FakeFunction]] = {
            collection_name(NAMESPACE, spec.name): {} for spec in plan.fleet.collections
        }
        for spec in plan.fleet.registration_order():
            self.get_collection(
                collection_name(NAMESPACE, spec.collection)
            ).register_function(None, None, spec.name, True)

    def get_collection(self, name: str) -> FakeCollection:
        return FakeCollection(self, name)

    def list_transactions(self) -> list:
        return list(self.transactions)

    def list_function_runs(self) -> list:
        return list(self.runs)

    def list_functions(self, name: str) -> list[FakeFunction]:
        with self.lock:
            return list(self.collections[name].values())


class FakeCluster:
    """Mirrors the server: one trigger workload per enabled cron or stream
    function, at its current version; `stale` keeps the workloads of deleted
    functions around, as a broken reconciler would."""

    def __init__(self, server: FakeServer, plan: Plan, stale: bool = False):
        self.server = server
        self.kinds = {spec.name: spec.kind for spec in plan.fleet.functions}
        self.stale = stale
        self.left: dict[str, SimpleNamespace] = {}
        self.errors: list[tuple[str, str, str]] = []

    def unfinished_in_project(self, project_id: str) -> list:
        items = []
        with self.server.lock:
            functions = [
                function
                for functions in self.server.collections.values()
                for function in functions.values()
            ]
        live = set()
        for function in functions:
            kind = self.kinds[function.name]
            if kind not in (CRON_PUBLISHER, STREAM_PUBLISHER) or not function.enabled:
                continue
            item = self.workload(function, runner.TRIGGER_DECORATORS[kind])
            live.add(item.metadata.name)
            items.append(item)
            if self.stale:
                self.left[function.kwargs["function_id"]] = item
        if self.stale:
            current = {function.kwargs["function_id"] for function in functions}
            items += [item for key, item in self.left.items() if key not in current]
        return items

    @staticmethod
    def workload(function: FakeFunction, decorator: str) -> SimpleNamespace:
        labels = {
            FUNCTION_ID: function.kwargs["function_id"],
            DECORATOR: decorator,
            FUNCTION_VERSION_ID: function.id,
        }
        return SimpleNamespace(
            metadata=SimpleNamespace(name=f"wl-{function.id}", labels=labels)
        )

    def jobs(self, selector: str) -> list:
        return []

    def cronjobs(self, selector: str) -> list:
        return []

    def server_errors(self, since_seconds: int) -> list[tuple[str, str, str]]:
        return list(self.errors)

    def unfinished_job_of_worker(self, worker_id: str):
        return None


def quick_plan(seed: int, operations: int = 300) -> Plan:
    plan = generate(seed, operations, lanes=4, faults=True, scale=0.5)
    return dataclasses.replace(
        plan, ops=tuple(dataclasses.replace(op, delay=0.0) for op in plan.ops)
    )


def run_against(plan: Plan, server: FakeServer, cluster: FakeCluster):
    module = SimpleNamespace(**{spec.name: object() for spec in plan.fleet.functions})
    return runner.MonkeyRun(
        plan=plan,
        namespace=NAMESPACE,
        connect=lambda: server,
        cluster=cluster,
        project=PROJECT,
        project_id=PROJECT_ID,
        project_root=Path("."),
        module=module,
    ).execute()


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(runner, "SETTLE_TIMEOUT", 1)
    monkeypatch.setattr(runner, "EXECUTIONS_TIMEOUT", 1)
    monkeypatch.setattr(runner, "POLL_SECONDS", 0.05)
    monkeypatch.setattr(runner, "MONITOR_SECONDS", 0.01)


def test_a_plan_is_reproducible_from_its_seed_and_from_its_descriptor(tmp_path):
    plan = generate(7, 1000, lanes=8, faults=True)
    assert generate(7, 1000, lanes=8, faults=True) == plan
    assert load_plan(write_plan(tmp_path / "plan.yaml", plan)) == plan


def test_every_function_gets_a_moderate_share_of_operations():
    plan = generate(11, 1000, lanes=8, faults=True)
    shares = Counter(op.function for op in plan.ops if op.function)
    assert len(plan.fleet.functions) >= 30
    assert set(shares) == {spec.name for spec in plan.fleet.functions}
    assert min(shares.values()) >= 10


def test_one_function_always_runs_on_the_same_lane():
    plan = generate(13, 1000, lanes=8, faults=True)
    lanes: dict[str, set[int]] = {}
    for op in plan.ops:
        if op.function:
            lanes.setdefault(op.function, set()).add(op.lane)
    assert all(len(used) == 1 for used in lanes.values())


def test_faults_can_be_left_out():
    plan = generate(17, 1000, lanes=8, faults=False)
    assert not [op for op in plan.ops if op.action in FAULTS]


def test_the_model_rejects_operations_on_deleted_functions_and_follows_outcomes():
    plan = generate(19, 10, lanes=1, faults=False)
    model = Model(plan.fleet)
    name = plan.fleet.functions[0].name
    delete, enable = Op(0, 0, 0.0, DELETE, name), Op(1, 0, 0.0, ENABLE, name)
    register = Op(2, 0, 0.0, REGISTER, name)
    assert model.predict(register) == REJECTED
    model.apply(delete, REJECTED)
    assert model.predict(enable) == ANY
    model.apply(delete, OK)
    assert model.predict(enable) == REJECTED
    assert model.predict(register) == ANY
    model.apply(register, OK)
    model.apply(Op(3, 0, 0.0, DISABLE, name), OK)
    assert model.expected()[name] == {"exists": True, "enabled": False}
    assert model.predict(Op(4, 0, 0.0, TRIGGER, name)) == REJECTED


def test_rejections_are_told_apart_from_server_errors():
    for status, result in ((404, REJECTED), (409, REJECTED), (500, ERROR)):
        try:
            rejected(status)
        except RuntimeError as error:
            assert runner.classify(error) == (result, status)
    assert runner.classify(ConnectionError("down")) == (ERROR, None)


def test_a_run_against_a_consistent_server_and_cluster_has_no_problem():
    plan = quick_plan(23)
    server = FakeServer(plan)
    report = run_against(plan, server, FakeCluster(server, plan))
    assert report.problems == []
    assert len(report.outcomes) == len(plan.ops)
    assert {outcome.result for outcome in report.outcomes} <= {OK, REJECTED, "skipped"}


def test_trigger_workloads_left_behind_by_deleted_functions_are_reported():
    plan = quick_plan(29, 600)
    server = FakeServer(plan)
    report = run_against(plan, server, FakeCluster(server, plan, stale=True))
    assert any("no longer lists" in problem for problem in report.problems)


def test_a_disable_the_server_acknowledges_but_drops_is_reported():
    plan = quick_plan(31, 600)
    server = FakeServer(plan, drift=True)
    report = run_against(plan, server, FakeCluster(server, plan))
    assert any("should be disabled" in problem for problem in report.problems)


def test_server_errors_on_requests_of_the_run_are_reported():
    plan = quick_plan(37)
    server = FakeServer(plan)
    cluster = FakeCluster(server, plan)
    cluster.errors = [
        ("GET", f"/api/v1/projects/{PROJECT}/collections", "500"),
        ("GET", "/api/v1/projects/someone_else/collections", "500"),
        ("GET", "/api/v1/internal/workers/wkr20261005-0000000072/message", "500"),
    ]
    report = run_against(plan, server, cluster)
    server_errors = [p for p in report.problems if p.startswith("The server answered")]
    assert server_errors == [
        f"The server answered 500 to GET /api/v1/projects/{PROJECT}/collections once"
    ]


def test_a_run_out_of_time_stops_and_reports_it():
    plan = generate(41, 200, lanes=4, faults=True, scale=0.5)
    server = FakeServer(plan)
    module = SimpleNamespace(**{spec.name: object() for spec in plan.fleet.functions})
    report = runner.MonkeyRun(
        plan=plan,
        namespace=NAMESPACE,
        connect=lambda: server,
        cluster=FakeCluster(server, plan),
        project=PROJECT,
        project_id=PROJECT_ID,
        project_root=Path("."),
        module=module,
        deadline=time.monotonic() + 2,
    ).execute()
    assert len(report.outcomes) < len(plan.ops)
    assert any("ran out of time" in problem for problem in report.problems)


def test_executions_that_never_finish_are_reported_with_their_details(monkeypatch):
    monkeypatch.setattr(runner, "STALL_SECONDS", 0.5)
    monkeypatch.setattr(runner, "NO_CHANGE_REPORT_SECONDS", 0.1)
    monkeypatch.setattr(runner, "EXECUTIONS_TIMEOUT", 30)
    plan = quick_plan(43, 120)
    server = FakeServer(plan, stuck=True)
    recorder = Recorder()
    module = SimpleNamespace(**{spec.name: object() for spec in plan.fleet.functions})
    report = runner.MonkeyRun(
        plan=plan,
        namespace=NAMESPACE,
        connect=lambda: server,
        cluster=FakeCluster(server, plan),
        project=PROJECT,
        project_id=PROJECT_ID,
        project_root=Path("."),
        module=module,
        progress=recorder,
    ).execute()
    said = recorder.messages
    assert any(line.startswith("No change; still waiting for") for line in said)
    assert any("giving up waiting" in line for line in said)
    assert any("never finished" in problem for problem in report.problems)
    assert any(line.startswith("Final state of") for line in said)
    assert any("transaction tx" in line and "Canceled" in line for line in said)


class Recorder:
    def __init__(self):
        self.messages: list[str] = []

    def __call__(self, message: str, tag: str = "run", concern: str | None = None):
        self.messages.append(message)

    def layout(self, *_):
        pass

    def operation(self, *_):
        pass


def test_a_transaction_running_although_its_runs_finished_is_reported():
    plan = quick_plan(47)
    server = FakeServer(plan)
    server.transactions = [
        SimpleNamespace(id="tx1", execution="ep1", status="Running"),
        SimpleNamespace(id="tx2", execution="ep1", status="Committed"),
        SimpleNamespace(id="tx3", execution="ep9", status="Running"),
    ]
    server.runs = [
        SimpleNamespace(transaction="tx1", function="tfr00", status="Done"),
        SimpleNamespace(transaction="tx1", function="tfr01", status="Canceled"),
        SimpleNamespace(transaction="tx2", function="pub00", status="Committed"),
        SimpleNamespace(transaction="tx3", function="tfr00", status="Done"),
    ]
    module = SimpleNamespace(**{spec.name: object() for spec in plan.fleet.functions})
    run = runner.MonkeyRun(
        plan=plan,
        namespace=NAMESPACE,
        connect=lambda: server,
        cluster=FakeCluster(server, plan),
        project=PROJECT,
        project_id=PROJECT_ID,
        project_root=Path("."),
        module=module,
    )
    run._executions["pub00"].append(SimpleNamespace(id="ep1", status=FINAL))
    run._check_transactions(server)
    assert run._problems == [
        "Transaction tx1 of execution ep1 is Running although all its function "
        "runs finished: tfr00 Done, tfr01 Canceled"
    ]
