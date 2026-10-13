#
# Copyright 2026 Tabsdata Inc.
#

"""Runs a monkey plan against a cluster instance and checks what it leaves.

While the lanes run, a monitor samples the run's project on the cluster and
records every moment two unfinished trigger workloads (cron jobs or streamers)
of one function coexist. A run's workload may still be exiting when the next
run of its function starts, so runs are checked on the server instead: no two
runs of one function may overlap in time. Afterwards the run settles: every
execution of the project, triggered by the run, a cron or a dependency, must
reach a final status, the functions the server reports must match the model,
and the cluster must hold exactly one trigger workload for each enabled cron or
stream function, at its current version, and no unfinished run of anything
else. Every triggered execution and its transactions must agree with their
parts (see ``rollups``). Any unexpected error from an operation, any 5xx in the
apiserver log other than a 503 (which tells a client to retry), and any
operation accepted on a function that should not exist, is a problem too.

From generator 5, cancels and recoveries aim at a random recent execution and
transaction; older plans replay as recorded."""

from __future__ import annotations

import random
import re
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType

import requests
from kubernetes import client as kube

from tabsdatak.sdk.status_utils.execution import EXECUTION_FINAL_STATUSES
from tabsdatak.sdk.status_utils.function_run import (
    FunctionRunStatus,
    function_run_status_to_mapping,
)
from tabsdatak.sdk.status_utils.transaction import (
    TRANSACTION_FINAL_STATUSES,
    TransactionStatus,
    transaction_status_to_mapping,
)
from tabsdatak.sdk.tabsdata_server import (
    Collection,
    Execution,
    Function,
    TabsdataServer,
)
from testi_tabsdatak.conn.common.cluster import (
    DECORATOR,
    FUNCTION_ID,
    FUNCTION_VERSION_ID,
    PROJECT_ID,
    WORKER_ID,
    Cluster,
    finished,
    label_value,
    orphan_spec,
)
from testi_tabsdatak.conn.common.monkey.held import held_transactions
from testi_tabsdatak.conn.common.monkey.model import (
    ERROR,
    OK,
    REJECTED,
    SKIPPED,
    Model,
)
from testi_tabsdatak.conn.common.monkey.naming import collection_name
from testi_tabsdatak.conn.common.monkey.plan import (
    CANCEL_EXECUTION,
    CANCEL_TRANSACTION,
    CRON_PUBLISHER,
    DELETE,
    DISABLE,
    ENABLE,
    LOSE_JOB,
    LOSE_STREAMER,
    ORPHAN,
    PAUSE,
    RECOVER_EXECUTION,
    RECOVER_TRANSACTION,
    REGISTER,
    STREAM_PUBLISHER,
    TRIGGER,
    UPDATE,
    FunctionSpec,
    Op,
    Plan,
)
from testi_tabsdatak.conn.common.monkey.progress import (
    BAD,
    FAILED,
    GOOD,
    OVERLAP,
    PASSED,
    PROBLEM,
    SETTLE,
    WAITING,
    Progress,
    counted,
    spoken,
)
from testi_tabsdatak.conn.common.monkey.rollups import (
    execution_problems,
    transaction_problems,
    worker_problems,
)

RUN_DECORATORS = ("publisher", "transformer", "subscriber")
TRIGGER_DECORATORS = {
    CRON_PUBLISHER: "cron_publisher",
    STREAM_PUBLISHER: "stream_publisher",
}
FUNCTION_RUN = "F"

EXECUTIONS_TIMEOUT = 14400
SETTLE_TIMEOUT = 900
POLL_SECONDS = 5
STALL_SECONDS = 600
NO_CHANGE_REPORT_SECONDS = 30
FINISHED_RUNS = {
    function_run_status_to_mapping(status.value)
    for status in (
        FunctionRunStatus.DONE,
        FunctionRunStatus.COMMITTED,
        FunctionRunStatus.CANCELED,
        FunctionRunStatus.FAILED,
        FunctionRunStatus.YANKED,
    )
}
TRANSACTION_RUNNING = transaction_status_to_mapping(TransactionStatus.RUNNING.value)
TRANSACTION_UNEXPECTED = transaction_status_to_mapping(
    TransactionStatus.UNEXPECTED.value
)
WORKER_TYPES = {"F": "function run", "CT": "cron trigger", "ST": "stream trigger"}
MONITOR_SECONDS = 2.0
DETAIL_LENGTH = 500
REPORTED_VIOLATIONS = 20
EXAMPLES_PER_KIND = 3

Connect = Callable[[], TabsdataServer]


@dataclass
class Outcome:
    index: int
    lane: int
    action: str
    function: str | None
    result: str
    status: int | None = None
    detail: str = ""
    elapsed: float = 0.0

    def to_dict(self) -> dict:
        return {
            key: value for key, value in asdict(self).items() if value not in (None, "")
        }


@dataclass
class Report:
    outcomes: list[Outcome]
    problems: list[str]
    expected: dict
    observed: dict
    duration: float = 0.0


def classify(error: BaseException) -> tuple[str, int | None]:
    cause = error if isinstance(error, requests.HTTPError) else error.__context__
    status = getattr(getattr(cause, "response", None), "status_code", None)
    if status is not None and status < 500:
        return REJECTED, status
    return ERROR, status


WORKER_ID_TEXT = re.compile(r"\b([a-z]+\d{8})-(\d+)\b")
LOG_MARGIN_SECONDS = 60
RETRY_STATUS = "503"
TARGETED_FROM_GENERATOR = 5
RECENT_EXECUTIONS = 5


def worker_keys(text: str) -> set[tuple[str, int]]:
    return {(day, int(counter)) for day, counter in WORKER_ID_TEXT.findall(text)}


def joined(items: list[str]) -> str:
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"


def cluster_state(expected: int, missing: int, unexpected: int, runs: int) -> str:
    if not missing and not unexpected and not runs:
        return (
            f"The cluster holds all {counted(expected, 'expected trigger workload')} "
            "and no unfinished runs"
        )
    parts = []
    if missing:
        parts.append(f"is missing {missing} of {counted(expected, 'trigger workload')}")
    if unexpected:
        parts.append(f"holds {counted(unexpected, 'unexpected trigger workload')}")
    if runs:
        parts.append(f"still has {counted(runs, 'unfinished run')}")
    return f"The cluster {joined(parts)}"


def trigger_text(trigger: tuple) -> str:
    name, decorator, function_id, version = trigger
    owner = f"function {name}" if name else "a function the server no longer lists"
    return f"{decorator} workload of {owner} (id {function_id}, version {version})"


def labels_of(item) -> dict:
    return item.metadata.labels or {}


class Lane:
    def __init__(self, server: TabsdataServer, namespace: str):
        self.server = server
        self.namespace = namespace
        self._collections: dict[str, Collection] = {}

    def collection(self, name: str) -> Collection:
        if name not in self._collections:
            self._collections[name] = self.server.get_collection(
                collection_name(self.namespace, name)
            )
        return self._collections[name]

    def function(self, spec: FunctionSpec) -> Function:
        return self.collection(spec.collection).get_function(spec.name)


class ProjectMonitor:
    def __init__(
        self, cluster: Cluster, project_id: str, ignored: set[str], progress: Progress
    ):
        self.cluster = cluster
        self.project_id = project_id
        self.ignored = ignored
        self.progress = progress
        self.violations: list[str] = []
        self._seen: set[tuple] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self) -> ProjectMonitor:
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        self._thread.join()

    def _sample(self) -> None:
        while not self._stop.wait(MONITOR_SECONDS):
            try:
                items = self.cluster.unfinished_in_project(self.project_id)
            except kube.ApiException:
                continue
            groups: dict[tuple[str, str], set[str]] = defaultdict(set)
            for item in items:
                if item.metadata.name in self.ignored:
                    continue
                labels = labels_of(item)
                groups[(labels.get(FUNCTION_ID, ""), labels.get(DECORATOR, ""))].add(
                    item.metadata.name
                )
            for (function_id, decorator), names in groups.items():
                if decorator in RUN_DECORATORS:
                    continue
                key = (function_id, decorator, frozenset(names))
                if len(names) > 1 and key not in self._seen:
                    self._seen.add(key)
                    violation = (
                        f"{counted(len(names), f'unfinished {decorator} workload')} "
                        f"of function {function_id} at {time.strftime('%H:%M:%S')}: "
                        f"{joined(sorted(names))}"
                    )
                    self.violations.append(violation)
                    self.progress(violation, OVERLAP)


class MonkeyRun:
    def __init__(
        self,
        plan: Plan,
        namespace: str,
        connect: Connect,
        cluster: Cluster,
        project: str,
        project_id: str,
        project_root: Path,
        module: ModuleType,
        progress: Progress | None = None,
        deadline: float | None = None,
    ):
        self.plan = plan
        self.namespace = namespace
        self.connect = connect
        self.cluster = cluster
        self.project = project
        self.project_id = project_id
        self.project_root = project_root
        self.module = module
        self.model = Model(plan.fleet)
        self.progress = progress or Progress()
        self.deadline = deadline
        self._done = 0
        self._lock = threading.Lock()
        self._outcomes: list[Outcome] = []
        self._problems: list[str] = []
        self._executions: dict[str, list[Execution]] = defaultdict(list)
        self._orphans: set[str] = set()
        self._unfinished: list[tuple[str, Execution]] = []

    def execute(self) -> Report:
        started = time.monotonic()
        self.progress.layout(
            len(self.plan.ops), (spec.name for spec in self.plan.fleet.functions)
        )
        self.progress(
            f"Running {counted(len(self.plan.ops), 'operation')} on "
            f"{counted(len(self.plan.fleet.functions), 'function')} in "
            f"{counted(self.plan.lanes, 'lane')} (namespace {self.namespace})"
        )
        with ProjectMonitor(
            self.cluster, self.project_id, self._orphans, self.progress
        ) as monitor:
            lanes = [
                threading.Thread(target=self._lane, args=(index,), daemon=True)
                for index in range(self.plan.lanes)
            ]
            for lane in lanes:
                lane.start()
            for lane in lanes:
                lane.join()
            self.progress(
                "All lanes finished; waiting for the system to settle", SETTLE
            )
            observed = self._settle()
        for violation in monitor.violations[:REPORTED_VIOLATIONS]:
            self._problem(f"Overlap: {violation}")
        if len(monitor.violations) > REPORTED_VIOLATIONS:
            hidden = len(monitor.violations) - REPORTED_VIOLATIONS
            self._problem(f"And {counted(hidden, 'more overlap')}")
        self._check_server_errors(time.monotonic() - started)
        self._report_executions()
        duration = time.monotonic() - started
        if self._problems:
            self.progress(
                f"Finished with {counted(len(self._problems), 'problem')} "
                f"in {spoken(duration)}",
                FAILED,
            )
        else:
            self.progress(f"Finished without problems in {spoken(duration)}", PASSED)
        return Report(
            outcomes=sorted(self._outcomes, key=lambda outcome: outcome.index),
            problems=list(self._problems),
            expected=self.model.expected(),
            observed=observed,
            duration=round(duration, 1),
        )

    def _check_server_errors(self, elapsed: float) -> None:
        workers = self._worker_keys()
        try:
            errors = self.cluster.server_errors(int(elapsed) + LOG_MARGIN_SECONDS)
        except kube.ApiException as error:
            self._problem(f"Could not read the apiserver log: {error.reason}")
            return
        ours: dict[tuple[str, str, str], int] = defaultdict(int)
        retried = 0
        for method, uri, status in errors:
            if (
                f"/projects/{self.project}/" in uri
                or self.project_id in uri
                or worker_keys(uri) & workers
            ):
                if status == RETRY_STATUS:
                    retried += 1
                else:
                    ours[(method, uri, status)] += 1
        if retried:
            self.progress(
                f"The server answered {counted(retried, 'request')} of this run "
                f"with {RETRY_STATUS}, telling the client to retry",
                SETTLE,
            )
        if not ours:
            self.progress(
                "The server answered no request of this run with a 5xx other than "
                f"{RETRY_STATUS}",
                SETTLE,
                GOOD,
            )
        for (method, uri, status), count in sorted(ours.items()):
            times = "once" if count == 1 else f"{count} times"
            self._problem(f"The server answered {status} to {method} {uri} {times}")

    def _worker_keys(self) -> set[tuple[str, int]]:
        selector = f"{PROJECT_ID}={label_value(self.project_id)}"
        items = self.cluster.jobs(selector) + self.cluster.cronjobs(selector)
        keys = {
            key
            for item in items
            for key in worker_keys(labels_of(item).get(WORKER_ID, ""))
        }
        with self._lock:
            executions = [
                execution
                for executions in self._executions.values()
                for execution in executions
            ]
        for execution in executions:
            for worker in execution.workers:
                keys |= worker_keys(worker.id)
        return keys

    def _remaining(self) -> float:
        if self.deadline is None:
            return float("inf")
        return max(0.0, self.deadline - time.monotonic())

    def _problem(self, message: str) -> None:
        with self._lock:
            self._problems.append(message)
        self.progress(message, PROBLEM)

    def _lane(self, index: int) -> None:
        try:
            lane = Lane(self.connect(), self.namespace)
            for op in self.plan.lane_ops(index):
                if self._remaining() <= op.delay:
                    self._problem(
                        f"Lane {index} ran out of time before operation {op.index}"
                    )
                    return
                time.sleep(op.delay)
                outcome = self._perform(lane, op)
                with self._lock:
                    self._outcomes.append(outcome)
                    self._done += 1
                    done = self._done
                self.progress.operation(
                    done,
                    len(self.plan.ops),
                    outcome.lane,
                    outcome.index,
                    outcome.action,
                    outcome.function,
                    outcome.result,
                    outcome.status,
                    outcome.elapsed,
                    outcome.detail,
                )
        except Exception as error:
            self._problem(f"Lane {index} stopped: {type(error).__name__}: {error}")

    def _perform(self, lane: Lane, op: Op) -> Outcome:
        predicted = self.model.predict(op)
        started = time.monotonic()
        status = None
        acted_on: list[str] = []
        try:
            skipped = self._act(lane, op, acted_on)
            result, detail = (SKIPPED, skipped) if skipped else (OK, "")
        except Exception as error:
            result, status = classify(error)
            detail = f"{type(error).__name__}: {error}"[:DETAIL_LENGTH]
        if acted_on:
            detail = "; ".join([*acted_on, detail]) if detail else "; ".join(acted_on)
        self.model.apply(op, result)
        target = f" {op.function}" if op.function else ""
        label = f"Operation {op.index} ({op.action}{target})"
        if result == ERROR:
            self._problem(f"{label} failed: {detail}")
        if predicted == REJECTED and result == OK:
            self._problem(f"{label} was accepted, but the model rejects it")
        return Outcome(
            index=op.index,
            lane=op.lane,
            action=op.action,
            function=op.function,
            result=result,
            status=status,
            detail=detail,
            elapsed=round(time.monotonic() - started, 2),
        )

    def _act(self, lane: Lane, op: Op, acted_on: list[str]) -> str | None:
        if op.action == PAUSE:
            return None
        if op.action == ORPHAN:
            return self._orphan(acted_on)
        spec = self.plan.fleet.function(op.function)
        info = getattr(self.module, spec.name)
        if op.action == REGISTER:
            lane.collection(spec.collection).register_function(
                info=info,
                project_root=str(self.project_root),
                function_name=spec.name,
                reuse_tables=True,
            )
        elif op.action == DELETE:
            lane.function(spec).delete()
        elif op.action in (ENABLE, DISABLE):
            lane.function(spec).enable(op.action == ENABLE)
        elif op.action == UPDATE:
            lane.collection(spec.collection).update_function(
                spec.name,
                info=info,
                project_root=str(self.project_root),
                enabled=self.model.enabled(spec.name),
            )
        elif op.action == TRIGGER:
            execution = lane.function(spec).trigger()
            acted_on.append(f"Execution {execution.id}")
            with self._lock:
                self._executions[spec.name].append(execution)
        elif op.action in (CANCEL_EXECUTION, RECOVER_EXECUTION):
            execution = self._target(op)
            if execution is None:
                return "No execution yet"
            acted_on.append(f"Execution {execution.id}")
            if op.action == CANCEL_EXECUTION:
                execution.cancel()
            else:
                execution.recover()
        elif op.action in (CANCEL_TRANSACTION, RECOVER_TRANSACTION):
            execution = self._target(op)
            transactions = execution.transactions if execution is not None else []
            if not transactions:
                return "No transaction yet"
            transaction = self._pick(op, transactions, transactions[0])
            acted_on.append(f"Transaction {transaction.id} of execution {execution.id}")
            if op.action == CANCEL_TRANSACTION:
                transaction.cancel()
            else:
                transaction.recover()
        elif op.action == LOSE_JOB:
            return self._lose_job(spec, acted_on)
        elif op.action == LOSE_STREAMER:
            return self._lose_streamer(lane, spec, acted_on)
        return None

    def _latest(self, function: str) -> Execution | None:
        with self._lock:
            executions = self._executions.get(function)
            return executions[-1] if executions else None

    def _target(self, op: Op) -> Execution | None:
        with self._lock:
            recent = self._executions.get(op.function, [])[-RECENT_EXECUTIONS:]
        if not recent:
            return None
        return self._pick(op, recent, recent[-1])

    def _pick(self, op: Op, items: list, fixed):
        if self.plan.generator < TARGETED_FROM_GENERATOR:
            return fixed
        return random.Random(f"{self.plan.seed}-{op.index}-{len(items)}").choice(items)

    def _lose_job(self, spec: FunctionSpec, acted_on: list[str]) -> str | None:
        execution = self._latest(spec.name)
        if execution is None:
            return "No execution yet"
        for worker in execution.workers:
            if worker.type != FUNCTION_RUN:
                continue
            job = self.cluster.unfinished_job_of_worker(worker.id)
            if job is not None:
                acted_on.append(f"Job {job.metadata.name} of execution {execution.id}")
                self.cluster.delete_job(job.metadata.name)
                return None
        return "No unfinished job"

    def _lose_streamer(
        self, lane: Lane, spec: FunctionSpec, acted_on: list[str]
    ) -> str | None:
        function_id = lane.function(spec).kwargs.get("function_id")
        selector = (
            f"{FUNCTION_ID}={label_value(function_id)},"
            f"{DECORATOR}={TRIGGER_DECORATORS[STREAM_PUBLISHER]}"
        )
        jobs = [job for job in self.cluster.jobs(selector) if not finished(job)]
        if not jobs:
            return "No streamer"
        acted_on.append(f"Job {jobs[0].metadata.name}")
        self.cluster.delete_job(jobs[0].metadata.name)
        return None

    def _orphan(self, acted_on: list[str]) -> str | None:
        jobs = sorted(
            (
                item
                for item in self.cluster.unfinished_in_project(self.project_id)
                if isinstance(item, kube.V1Job)
                and labels_of(item).get(DECORATOR) in RUN_DECORATORS
                and item.metadata.name not in self._orphans
            ),
            key=lambda job: job.metadata.name,
        )
        if not jobs:
            return "No run job to copy"
        name, labels, annotations, image = orphan_spec(jobs[0])
        acted_on.append(f"Job {name}, copied from {jobs[0].metadata.name}")
        try:
            self.cluster.create_suspended_job(name, labels, annotations, image)
        except kube.ApiException as error:
            if error.status != 409:
                raise
            return "Already orphaned"
        self._orphans.add(name)
        return None

    def _settle(self) -> dict:
        server = self.connect()
        others = self._untracked_executions(server)
        self._await_executions()
        self._await_untracked(others)
        self._check_transactions(server)
        self._check_run_overlaps(server)
        functions = self._functions(server)
        self.progress(
            f"The server lists {counted(len(functions), 'function')}; "
            "comparing them with the expected state",
            SETTLE,
        )
        before = len(self._problems)
        self._compare_functions(functions)
        if len(self._problems) == before:
            self.progress("Every function matches the expected state", SETTLE, GOOD)
        expected = self._expected_triggers(functions)
        names = {
            function.kwargs.get("function_id"): name
            for name, function in functions.items()
        }
        cron_ids = {
            function_id
            for function_id, name in names.items()
            if self.plan.fleet.function(name).kind == CRON_PUBLISHER
        }
        deadline = time.monotonic() + min(SETTLE_TIMEOUT, self._remaining())
        reported = None
        while True:
            items = self.cluster.unfinished_in_project(self.project_id)
            triggers = self._observed_triggers(items, names)
            stray = sorted(
                item.metadata.name
                for item in items
                if labels_of(item).get(DECORATOR) in RUN_DECORATORS
                and labels_of(item).get(FUNCTION_ID) not in cron_ids
            )
            state = (
                len(expected - triggers),
                len(triggers - expected),
                len(stray),
            )
            if state != reported:
                reported = state
                self.progress(
                    cluster_state(len(expected), *state),
                    SETTLE,
                    GOOD if not any(state) else WAITING,
                )
            if (triggers == expected and not stray) or time.monotonic() >= deadline:
                break
            time.sleep(POLL_SECONDS)
        for missing in sorted(expected - triggers):
            self._problem(f"Missing the {trigger_text(missing)}")
        for unexpected in sorted(triggers - expected):
            self._problem(f"Unexpected {trigger_text(unexpected)}")
        if stray:
            self._problem(f"Unfinished runs left behind: {joined(stray)}")
        return {
            "functions": {
                name: {
                    "enabled": function.enabled,
                    "version": function.id,
                    "function_id": function.kwargs.get("function_id"),
                }
                for name, function in sorted(functions.items())
            },
            "workloads": [
                {
                    "name": item.metadata.name,
                    "decorator": labels_of(item).get(DECORATOR),
                    "function": names.get(labels_of(item).get(FUNCTION_ID)),
                    "function_id": labels_of(item).get(FUNCTION_ID),
                    "version": labels_of(item).get(FUNCTION_VERSION_ID),
                }
                for item in sorted(items, key=lambda item: item.metadata.name)
            ],
        }

    def _tracked(self) -> list[tuple[str, Execution]]:
        with self._lock:
            return [
                (function, execution)
                for function, executions in self._executions.items()
                for execution in executions
            ]

    def _untracked_executions(
        self, server: TabsdataServer
    ) -> list[tuple[str, Execution]]:
        tracked = {execution.id for _, execution in self._tracked()}
        return [
            (f"{execution.function} ({execution.trigger_type})", execution)
            for execution in server.list_executions()
            if execution.id not in tracked
            and execution.status not in EXECUTION_FINAL_STATUSES
        ]

    def _await_untracked(self, others: list[tuple[str, Execution]]) -> None:
        if not others:
            return
        self.progress(
            f"Waiting for {counted(len(others), 'execution')} triggered by crons "
            "or dependencies to finish",
            SETTLE,
            WAITING,
        )
        unfinished = self._unfinished
        self._await_executions(others, "untriggered")
        self._unfinished = unfinished + self._unfinished

    def _check_run_overlaps(self, server: TabsdataServer) -> None:
        by_function: dict[str, list] = defaultdict(list)
        for run in server.list_function_runs():
            if run.started_on is not None:
                by_function[run.kwargs.get("function_id")].append(run)
        overlaps = 0
        for runs in by_function.values():
            runs.sort(key=lambda run: run.started_on)
            for before, after in zip(runs, runs[1:]):
                if before.ended_on is None or after.started_on < before.ended_on:
                    overlaps += 1
                    self._problem(
                        f"Runs {before.id} and {after.id} of {before.function} "
                        f"overlap: the second started before the first ended"
                    )
        if not overlaps:
            self.progress(
                "No two runs of one function overlapped in time", SETTLE, GOOD
            )

    def _await_executions(
        self,
        pending: list[tuple[str, Execution]] | None = None,
        label: str = "triggered",
    ) -> None:
        if pending is None:
            pending = self._tracked()
        deadline = time.monotonic() + min(EXECUTIONS_TIMEOUT, self._remaining())
        reported = None
        changed = reported_at = time.monotonic()
        while pending and time.monotonic() < deadline:
            now = time.monotonic()
            if len(pending) != reported:
                reported = len(pending)
                changed = reported_at = now
                self.progress(
                    f"Waiting for {counted(reported, 'execution')} to finish",
                    SETTLE,
                    WAITING,
                )
            elif now - changed >= STALL_SECONDS:
                self.progress(
                    f"No execution finished in {spoken(STALL_SECONDS)}; "
                    "giving up waiting",
                    SETTLE,
                    BAD,
                )
                break
            elif now - reported_at >= NO_CHANGE_REPORT_SECONDS:
                reported_at = now
                self.progress(
                    f"No change; still waiting for {counted(reported, 'execution')} "
                    "to finish",
                    SETTLE,
                    WAITING,
                )
                for line in self._execution_lines(pending):
                    self.progress(line, SETTLE)
                held = self._held_executions({execution.id for _, execution in pending})
                if held:
                    self.progress(
                        f"{counted(len(held), 'execution')} wait to commit behind a "
                        "stalled transaction; counting them as settled",
                        SETTLE,
                    )
                    pending = [item for item in pending if item[1].id not in held]
                    reported = len(pending)
            still = []
            for function, execution in pending:
                try:
                    execution.refresh()
                except Exception:
                    still.append((function, execution))
                    continue
                if execution.status not in EXECUTION_FINAL_STATUSES:
                    still.append((function, execution))
            pending = still
            if pending:
                time.sleep(POLL_SECONDS)
        self._unfinished = pending
        if not pending:
            self.progress(f"Every {label} execution has finished", SETTLE, GOOD)
        for function, execution in pending:
            self._problem(
                f"Execution {execution.id} of {function} never finished "
                f"(last status {execution.status!r})"
            )

    def _parts(
        self, server: TabsdataServer, execution_ids: set[str]
    ) -> tuple[list, dict[str, list]]:
        transactions = [
            transaction
            for transaction in server.list_transactions()
            if transaction.execution.id in execution_ids
        ]
        ids = {transaction.id for transaction in transactions}
        runs: dict[str, list] = defaultdict(list)
        for run in server.list_function_runs():
            if run.transaction.id in ids:
                runs[run.transaction.id].append(run)
        return transactions, runs

    def _upstream(self, function: str) -> str | None:
        spec = next(
            (spec for spec in self.plan.fleet.functions if spec.name == function), None
        )
        return spec.upstream if spec else None

    def _held(self, transactions: list, runs: dict[str, list]) -> set[str]:
        return held_transactions(transactions, runs, self._upstream)

    def _held_executions(self, execution_ids: set[str]) -> set[str]:
        """Executions whose unfinished transactions all wait to commit behind a
        stalled one."""
        try:
            transactions, runs = self._parts(self.connect(), execution_ids)
        except Exception:
            return set()
        held = self._held(transactions, runs)
        by_execution: dict[str, list] = defaultdict(list)
        for transaction in transactions:
            by_execution[transaction.execution.id].append(transaction)
        return {
            execution_id
            for execution_id, parts in by_execution.items()
            if any(transaction.id in held for transaction in parts)
            and all(
                transaction.id in held
                or transaction.status in TRANSACTION_FINAL_STATUSES
                for transaction in parts
            )
        }

    def _check_transactions(self, server: TabsdataServer) -> None:
        tracked = {execution.id for _, execution in self._tracked()}
        transactions, runs = self._parts(server, tracked)
        held = self._held(transactions, runs)
        if held:
            self.progress(
                f"{counted(len(held), 'transaction')} of the triggered executions "
                "wait to commit behind a stalled transaction",
                SETTLE,
            )
        stuck = 0
        for transaction in transactions:
            statuses = {run.status for run in runs[transaction.id]}
            finished = bool(statuses) and statuses <= FINISHED_RUNS
            if transaction.status == TRANSACTION_UNEXPECTED or (
                transaction.status == TRANSACTION_RUNNING
                and finished
                and transaction.id not in held
            ):
                stuck += 1
                listed = ", ".join(
                    f"{run.function} {run.status}" for run in runs[transaction.id]
                )
                self._problem(
                    f"Transaction {transaction.id} of execution "
                    f"{transaction.execution.id} is {transaction.status} although all "
                    f"its function runs finished: {listed}"
                )
        if not stuck:
            self.progress(
                f"All {counted(len(transactions), 'transaction')} of the triggered "
                "executions have a status that matches their function runs",
                SETTLE,
                GOOD,
            )
        self._check_rollups(server, tracked, transactions, runs)

    def _check_rollups(
        self,
        server: TabsdataServer,
        tracked: set[str],
        transactions: list,
        runs: dict[str, list],
    ) -> None:
        problems = []
        by_execution: dict[str, list] = defaultdict(list)
        for transaction in transactions:
            problems += transaction_problems(transaction, runs[transaction.id])
            by_execution[transaction.execution.id].append(transaction)
        executions = [
            execution
            for execution in server.list_executions()
            if execution.id in tracked
        ]
        for execution in executions:
            problems += execution_problems(execution, by_execution[execution.id])
            problems += worker_problems(execution, execution.workers)
        by_kind: dict[str, list[str]] = defaultdict(list)
        for problem in problems:
            by_kind[problem.kind].append(problem.text)
        for kind, texts in by_kind.items():
            examples = "; ".join(texts[:EXAMPLES_PER_KIND])
            self._problem(f"{kind}: {len(texts)}, for example {examples}")
        if not problems:
            self.progress(
                f"All {counted(len(executions), 'triggered execution')} span their "
                "transactions, end only once all their parts end, and leave no live "
                "function worker",
                SETTLE,
                GOOD,
            )

    def _execution_lines(self, executions: list[tuple[str, Execution]]) -> list[str]:
        lines = []
        for function, execution in executions:
            try:
                lines += self._describe_execution(function, execution)
            except Exception as error:
                lines.append(
                    f"Execution {execution.id} of {function}: could not read its "
                    f"details ({type(error).__name__}: {error})"
                )
        return lines

    @staticmethod
    def _describe_execution(function: str, execution: Execution) -> list[str]:
        lines = [f"Execution {execution.id} of {function} is {execution.status}"]
        for transaction in execution.transactions:
            runs = ", ".join(
                f"{getattr(run.function, 'name', run.function)} {run.status}"
                for run in transaction.function_runs
            )
            lines.append(
                f"    transaction {transaction.id} is {transaction.status}: {runs}"
            )
        for worker in execution.workers:
            kind = WORKER_TYPES.get(worker.type, worker.type)
            owner = getattr(worker.function, "name", worker.function)
            lines.append(
                f"    worker {worker.id} ({kind} of {owner}) is {worker.status}"
            )
        return lines

    def _report_executions(self) -> None:
        tracked = self._tracked()
        statuses = Counter(execution.status for _, execution in tracked)
        summary = ", ".join(
            f"{count} {status}" for status, count in statuses.most_common()
        )
        if not self._unfinished:
            self.progress(
                f"Final state of {counted(len(tracked), 'triggered execution')}: "
                f"{summary or 'none'}",
                SETTLE,
                GOOD,
            )
            return
        self.progress(
            f"Final state of {counted(len(tracked), 'triggered execution')}: "
            f"{summary}; {counted(len(self._unfinished), 'execution')} never "
            "finished",
            SETTLE,
            BAD,
        )
        for line in self._execution_lines(self._unfinished):
            self.progress(line, SETTLE)

    def _functions(self, server: TabsdataServer) -> dict[str, Function]:
        functions: dict[str, Function] = {}
        for collection in self.plan.fleet.collections:
            for function in server.list_functions(
                collection_name(self.namespace, collection.name)
            ):
                functions[function.name] = function
        return functions

    def _compare_functions(self, functions: dict[str, Function]) -> None:
        for name, expected in self.model.expected().items():
            function = functions.get(name)
            if not expected["exists"]:
                if function is not None:
                    self._problem(
                        f"Function {name} should be deleted, but the server lists it"
                    )
                continue
            if function is None:
                self._problem(
                    f"Function {name} should exist, but the server does not list it"
                )
            elif bool(function.enabled) != expected["enabled"]:
                wanted = "enabled" if expected["enabled"] else "disabled"
                self._problem(
                    f"Function {name} should be {wanted}, "
                    f"but the server reports enabled={function.enabled}"
                )

    def _expected_triggers(self, functions: dict[str, Function]) -> set[tuple]:
        expected = set()
        for spec in self.plan.fleet.of_kind(CRON_PUBLISHER, STREAM_PUBLISHER):
            function = functions.get(spec.name)
            if function is None or not function.enabled:
                continue
            expected.add((
                spec.name,
                TRIGGER_DECORATORS[spec.kind],
                function.kwargs.get("function_id"),
                function.id,
            ))
        return expected

    def _observed_triggers(self, items: list, names: dict[str, str]) -> set[tuple]:
        observed = set()
        for item in items:
            labels = labels_of(item)
            decorator = labels.get(DECORATOR)
            if decorator not in TRIGGER_DECORATORS.values():
                continue
            function_id = labels.get(FUNCTION_ID)
            observed.add((
                names.get(function_id, ""),
                decorator,
                function_id,
                labels.get(FUNCTION_VERSION_ID),
            ))
        return observed
