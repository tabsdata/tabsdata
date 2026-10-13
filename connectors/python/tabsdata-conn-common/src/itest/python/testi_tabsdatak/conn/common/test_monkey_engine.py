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
    server_errors_in,
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
    RECOVER_EXECUTION,
    REGISTER,
    STREAM_PUBLISHER,
    TRIGGER,
    Op,
    Plan,
    generate,
)
from testi_tabsdatak.conn.common.monkey.rollups import (
    EARLY_END,
    EARLY_END_TIME,
    LATE_START,
    LIVE_WORKER,
    NO_END,
    SPAN,
    Problem,
    execution_problems,
    transaction_problems,
    worker_problems,
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
        self.executions: list = []
        self.collections: dict[str, dict[str, FakeFunction]] = {
            collection_name(NAMESPACE, spec.name): {} for spec in plan.fleet.collections
        }
        for spec in plan.fleet.registration_order():
            self.get_collection(
                collection_name(NAMESPACE, spec.collection)
            ).register_function(None, None, spec.name, True)

    def get_collection(self, name: str) -> FakeCollection:
        return FakeCollection(self, name)

    def list_executions(self) -> list:
        return list(self.executions)

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
        ("POST", f"/api/v1/projects/{PROJECT}/functions", "503"),
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


def part(id: str, status: str, started=None, ended=None, **fields):
    return SimpleNamespace(
        id=id, status=status, started_on=started, ended_on=ended, **fields
    )


def test_a_transaction_running_although_its_runs_finished_is_reported():
    plan = quick_plan(47)
    server = FakeServer(plan)
    server.transactions = [
        part("tx1", "Running", execution=SimpleNamespace(id="ep1")),
        part("tx2", "Committed", 1, 2, execution=SimpleNamespace(id="ep1")),
        part("tx3", "Running", execution=SimpleNamespace(id="ep9")),
    ]
    server.runs = [
        part("fr", "Done", transaction=SimpleNamespace(id="tx1"), function="tfr00"),
        part("fr", "Canceled", transaction=SimpleNamespace(id="tx1"), function="tfr01"),
        part(
            "fr", "Committed", transaction=SimpleNamespace(id="tx2"), function="pub00"
        ),
        part("fr", "Done", transaction=SimpleNamespace(id="tx3"), function="tfr00"),
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


def test_a_transaction_waiting_to_commit_behind_a_stalled_one_is_not_reported():
    plan = quick_plan(47)
    reader = next(spec for spec in plan.fleet.functions if spec.upstream)
    server = FakeServer(plan)
    server.transactions = [
        part("tx1", "Stalled", execution=SimpleNamespace(id="ep1")),
        part("tx2", "Running", execution=SimpleNamespace(id="ep2")),
    ]
    server.runs = [
        part("fr", "Done", transaction=SimpleNamespace(id="tx1"), function=reader.name),
        part("fr", "Done", transaction=SimpleNamespace(id="tx2"), function=reader.name),
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
    for id in ("ep1", "ep2"):
        run._executions[reader.name].append(SimpleNamespace(id=id, status=FINAL))
    run._check_transactions(server)
    assert not any("although all" in problem for problem in run._problems)
    assert run._held_executions({"ep1", "ep2"}) == {"ep2"}


def run_overlaps(runs: list) -> list[str]:
    plan = quick_plan(53)
    server = FakeServer(plan)
    server.runs = runs
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
    run._check_run_overlaps(server)
    return run._problems


def function_run(id: str, function_id: str, started: int, ended: int | None):
    return SimpleNamespace(
        id=id,
        function="pub00",
        started_on=started,
        ended_on=ended,
        kwargs={"function_id": function_id},
    )


def test_runs_of_one_function_that_overlap_are_reported():
    assert run_overlaps([
        function_run("fr1", "f1", 100, 200),
        function_run("fr2", "f1", 150, 300),
        function_run("fr3", "f2", 150, 300),
    ]) == [
        "Runs fr1 and fr2 of pub00 overlap: the second started before the first ended"
    ]


def test_runs_of_one_function_back_to_back_do_not_overlap():
    assert not run_overlaps([
        function_run("fr1", "f1", 100, 200),
        function_run("fr2", "f1", 200, 300),
        function_run("fr3", "f1", 400, None),
    ])


def test_an_execution_the_run_did_not_trigger_that_never_finishes_is_reported(
    monkeypatch,
):
    monkeypatch.setattr(runner, "STALL_SECONDS", 0.5)
    monkeypatch.setattr(runner, "POLL_SECONDS", 0.1)
    plan = quick_plan(59, 60)
    server = FakeServer(plan)
    server.executions = [
        SimpleNamespace(
            id="ep77",
            function="crn00",
            trigger_type="S",
            status="Running",
            refresh=lambda: None,
            transactions=[],
            workers=[],
        )
    ]
    report = run_against(plan, server, FakeCluster(server, plan))
    assert "Execution ep77 of crn00 (S) never finished (last status 'Running')" in (
        report.problems
    )


def picks(plan: Plan, executions: list) -> list:
    module = SimpleNamespace(**{spec.name: object() for spec in plan.fleet.functions})
    server = FakeServer(plan)
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
    run._executions["pub00"] = executions
    return [
        run._target(Op(index, 0, 0.0, RECOVER_EXECUTION, "pub00")).id
        for index in range(40)
    ]


def test_plans_before_generator_5_aim_at_the_latest_execution():
    plan = dataclasses.replace(quick_plan(59), generator=4)
    executions = [FakeExecution() for _ in range(8)]
    assert set(picks(plan, executions)) == {executions[-1].id}


def test_newer_plans_aim_at_recent_executions_the_same_way_on_every_replay():
    plan = quick_plan(59)
    executions = [FakeExecution() for _ in range(8)]
    chosen = picks(plan, executions)
    assert chosen == picks(plan, executions)
    recent = {execution.id for execution in executions[-runner.RECENT_EXECUTIONS :]}
    assert set(chosen) == recent


def test_a_descriptor_keeps_the_generator_it_was_drawn_with(tmp_path):
    plan = dataclasses.replace(quick_plan(61), generator=4)
    assert load_plan(write_plan(tmp_path / "plan.yaml", plan)).generator == 4


def test_a_transaction_ends_once_all_its_runs_end_and_spans_them():
    execution = SimpleNamespace(id="ep1")
    name = "Transaction tx1 of execution ep1"
    runs = [part("fr1", "Committed", 10, 20), part("fr2", "Canceled", 12, 30)]
    assert (
        transaction_problems(part("tx1", "Committed", 5, 30, execution=execution), runs)
        == []
    )
    assert transaction_problems(
        part("tx1", "Committed", 5, None, execution=execution), runs
    ) == [
        Problem(NO_END, f"{name} is Committed with all its parts ended, but has no end")
    ]
    assert transaction_problems(
        part("tx1", "Committed", 11, 25, execution=execution), runs
    ) == [
        Problem(
            LATE_START,
            f"{name} started at 11, after its first function run started at 10",
        ),
        Problem(
            EARLY_END_TIME,
            f"{name} ended at 25, before its last function run ended at 30",
        ),
    ]
    running = [part("fr1", "Committed", 10, 20), part("fr2", "Running", 12)]
    assert transaction_problems(
        part("tx1", "Running", 10, 20, execution=execution), running
    ) == [
        Problem(EARLY_END, f"{name} ended at 20 although parts of it are still Running")
    ]


def test_an_execution_spans_exactly_its_transactions():
    transactions = [part("tx1", "Committed", 10, 20), part("tx2", "Canceled", 5, 30)]
    assert execution_problems(part("ep1", "Finished", 5, 30), transactions) == []
    assert execution_problems(part("ep1", "Finished", 4, 31), transactions) == [
        Problem(
            SPAN, "Execution ep1 started at 4, but its first transaction started at 5"
        ),
        Problem(
            SPAN, "Execution ep1 ended at 31, but its last transaction ended at 30"
        ),
    ]
    assert execution_problems(
        part("ep1", "Failed", 5, 30), [part("tx1", "Stalled", 5)]
    ) == [
        Problem(
            EARLY_END,
            "Execution ep1 ended at 30 although parts of it are still Stalled",
        ),
        Problem(
            SPAN, "Execution ep1 ended at 30, but its last transaction ended at None"
        ),
    ]


def test_a_live_function_worker_of_a_settled_execution_is_reported():
    execution = part("ep1", "Finished")
    workers = [
        SimpleNamespace(id="w1", type="F", status="Running"),
        SimpleNamespace(id="w2", type="F", status="Done"),
        SimpleNamespace(id="w3", type="CT", status="Running"),
    ]
    assert worker_problems(execution, workers) == [
        Problem(
            LIVE_WORKER,
            "Worker w1 of execution ep1 is still Running although the execution is "
            "Finished",
        )
    ]


def test_settling_checks_the_roll_ups_of_triggered_executions():
    plan = quick_plan(71)
    server = FakeServer(plan)
    execution = part("ep1", "Finished", 5, 30, workers=[])
    server.executions = [execution, part("ep9", "Finished", 1, 2, workers=[])]
    server.transactions = [
        part("tx1", "Committed", 5, 20, execution=SimpleNamespace(id="ep1"))
    ]
    server.runs = [
        part(
            "fr1",
            "Committed",
            5,
            20,
            transaction=SimpleNamespace(id="tx1"),
            function="pub00",
        )
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
    run._executions["pub00"].append(execution)
    run._check_transactions(server)
    assert run._problems == [
        "Span differs from its transactions: 1, for example Execution ep1 ended at 30, "
        "but its last transaction ended at 20"
    ]


def test_each_kind_of_roll_up_problem_is_reported_once_with_examples():
    plan = quick_plan(73)
    server = FakeServer(plan)
    executions = [part(f"ep{i}", "Finished", 5, None, workers=[]) for i in range(5)]
    server.executions = executions
    server.transactions = [
        part(f"tx{i}", "Canceled", execution=SimpleNamespace(id=f"ep{i}"))
        for i in range(5)
    ]
    server.runs = [
        part(
            f"fr{i}",
            "Canceled",
            transaction=SimpleNamespace(id=f"tx{i}"),
            function="pub00",
        )
        for i in range(5)
    ]
    executions[4].workers = [SimpleNamespace(id="w9", type="F", status="Run Requested")]
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
    run._executions["pub00"].extend(executions)
    run._check_transactions(server)
    assert [problem.split(":")[0] for problem in run._problems] == [
        NO_END,
        SPAN,
        LIVE_WORKER,
    ]
    assert run._problems[0].startswith(f"{NO_END}: 10, for example ")
    assert run._problems[0].count("; ") == runner.EXAMPLES_PER_KIND - 1


def test_every_server_error_in_a_log_is_found():
    log = "\n".join(
        f"ERROR request{{id=r{i} method=POST uri=/api/v1/x{i} version=HTTP/1.1}}: "
        f"tower_http::trace::on_failure: response failed classification=Status code: "
        f"{status} Service Unavailable latency=1 ms"
        for i, status in enumerate(("503", "500", "503"))
    )
    assert server_errors_in(log) == [
        ("POST", "/api/v1/x0", "503"),
        ("POST", "/api/v1/x1", "500"),
        ("POST", "/api/v1/x2", "503"),
    ]
