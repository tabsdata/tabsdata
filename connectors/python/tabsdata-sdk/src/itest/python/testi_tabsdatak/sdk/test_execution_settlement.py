#
# Copyright 2026 Tabsdata Inc.
#

"""How executions settle on a cluster instance when function runs fail or are
canceled, with one publisher feeding two transformers and their subscribers.
An execution here spans three transactions (sources, transformers,
destinations), and the transformers' transaction holds two function runs.

- A transformer that fails stalls its transaction and execution; canceling
  the transaction then finishes the execution, and recovering it runs the
  failing function again.
- Canceling an execution must not leave a later execution, whose runs read
  state the canceled one was going to write, with a transaction that holds
  both a canceled and a finished run and so can never settle.
- A run that finished next to a canceled one in the same transaction must
  end published (Committed) or discarded (Canceled), never stay Done.
- An execution waiting for a failed run to be recovered or canceled stays
  Stalled when one of its other transactions is canceled.
- Triggering the publisher repeatedly while canceling recent executions and
  transactions must never leave a transaction ``Running`` once all its
  function runs have finished. When it does, the failure lists that
  transaction and every operation that touched its execution, in order.

Enterprise only: the transformers and subscribers run because their input
tables change.
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn, TrashDestConn
from tabsdatak.extensions.variant.edition import ENTERPRISE
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    ExecutionStatus,
    execution_status_to_mapping,
)
from tabsdatak.sdk.status_utils.function_run import (
    FunctionRunStatus,
    function_run_status_to_mapping,
)
from tabsdatak.sdk.status_utils.transaction import (
    TransactionStatus,
    transaction_status_to_mapping,
)
from tabsdatak.sdk.tabsdata_server import Collection, Execution, Function, Transaction
from testi_tabsdatak.conn.common.function_project import function_module

pytestmark = [
    pytest.mark.sdk,
    pytest.mark.slow,
    pytest.mark.skipif(
        not ENTERPRISE,
        reason="transformers and subscribers must run as their inputs change",
    ),
]

FUNCTIONS = Path(__file__).parent / "functions"
settlement = function_module(FUNCTIONS, "settlement")

STALLED = execution_status_to_mapping(ExecutionStatus.STALLED.value)
FINISHED = execution_status_to_mapping(ExecutionStatus.FINISHED.value)
TRANSACTION_STALLED = transaction_status_to_mapping(TransactionStatus.STALLED.value)
TRANSACTION_CANCELED = transaction_status_to_mapping(TransactionStatus.CANCELED.value)
TRANSACTION_RUNNING = transaction_status_to_mapping(TransactionStatus.RUNNING.value)
TRANSACTION_UNEXPECTED = transaction_status_to_mapping(
    TransactionStatus.UNEXPECTED.value
)
DONE = function_run_status_to_mapping(FunctionRunStatus.DONE.value)
COMMITTED = function_run_status_to_mapping(FunctionRunStatus.COMMITTED.value)
FAILED = function_run_status_to_mapping(FunctionRunStatus.FAILED.value)
ON_HOLD = function_run_status_to_mapping(FunctionRunStatus.ON_HOLD.value)
CANCELED = function_run_status_to_mapping(FunctionRunStatus.CANCELED.value)
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

STALL_TIMEOUT = 600
SETTLE_TIMEOUT = 900
NO_PROGRESS_SECONDS = 600
STUCK_SECONDS = 120
POLL_SECONDS = 5

ROUNDS = int(os.environ.get("SETTLEMENT_ROUNDS", "20"))
SEED = os.environ.get("SETTLEMENT_SEED")
RECENT = 3


def register(factory: CollectionFactory, names: list[str]) -> dict[str, Function]:
    collections = {
        settlement.SOURCES: factory(
            "sources", DataGenSrcConn(), name=settlement.SOURCES
        ),
        settlement.TRANSFORMERS: factory("default", name=settlement.TRANSFORMERS),
        settlement.DESTINATIONS: factory(
            "destinations", TrashDestConn(), name=settlement.DESTINATIONS
        ),
    }
    homes = {
        "ticks": settlement.SOURCES,
        "left": settlement.TRANSFORMERS,
        "right": settlement.TRANSFORMERS,
        "failing": settlement.TRANSFORMERS,
        "after_left": settlement.DESTINATIONS,
        "after_right": settlement.DESTINATIONS,
        "after_failing": settlement.DESTINATIONS,
    }
    functions = {}
    for name in names:
        collection: Collection = collections[homes[name]]
        functions[name] = collection.register_function(
            info=getattr(settlement, name),
            project_root=str(FUNCTIONS),
            function_name=name,
        )
    return functions


def run_name(run) -> str:
    return getattr(run.function, "name", run.function)


def runs_of(transaction: Transaction) -> dict[str, str]:
    return {run_name(run): run.status for run in transaction.function_runs}


def transaction_holding(execution: Execution, function: str) -> Transaction:
    execution.refresh()
    for transaction in execution.transactions:
        if function in runs_of(transaction):
            return transaction
    raise AssertionError(f"no transaction of {execution.id} holds {function}")


def wait_for_status(execution: Execution, statuses: set[str], timeout: float) -> str:
    deadline = time.monotonic() + timeout
    while True:
        execution.refresh()
        if execution.status in statuses:
            return execution.status
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"execution {execution.id} is {execution.status}, "
                f"not {' or '.join(sorted(statuses))}, after {timeout:.0f} s:\n"
                + describe(execution)
            )
        time.sleep(POLL_SECONDS)


def describe(execution: Execution) -> str:
    execution.refresh()
    lines = [f"execution {execution.id} is {execution.status}"]
    for transaction in execution.transactions:
        runs = ", ".join(
            f"{name} {status}" for name, status in runs_of(transaction).items()
        )
        lines.append(f"  transaction {transaction.id} is {transaction.status}: {runs}")
    return "\n".join(lines)


def attempts_of(execution: Execution, function: str) -> int:
    execution.refresh()
    return sum(
        1
        for worker in execution.workers
        if getattr(worker.function, "name", worker.function) == function
    )


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1200)
def test_a_failing_transformer_stalls_until_its_transaction_is_canceled(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    functions = register(
        collection_factory, ["ticks", "left", "failing", "after_left", "after_failing"]
    )
    execution = functions["ticks"].trigger()
    try:
        wait_for_status(execution, {STALLED}, STALL_TIMEOUT)
        transformers = transaction_holding(execution, "failing")
        assert transformers.status == TRANSACTION_STALLED, describe(execution)
        assert runs_of(transformers) == {"left": DONE, "failing": FAILED}
        destinations = transaction_holding(execution, "after_failing")
        assert runs_of(destinations)["after_failing"] == ON_HOLD, describe(execution)

        transformers.cancel()

        wait_for_status(execution, {FINISHED}, STALL_TIMEOUT)
        for transaction in execution.transactions:
            assert transaction.status not in (
                TRANSACTION_RUNNING,
                TRANSACTION_UNEXPECTED,
            ), describe(execution)
        transformers = transaction_holding(execution, "failing")
        assert transformers.status == TRANSACTION_CANCELED, describe(execution)
        assert set(runs_of(transformers).values()) == {CANCELED}
        destinations = transaction_holding(execution, "after_failing")
        assert runs_of(destinations)["after_failing"] == CANCELED, describe(execution)
    finally:
        execution.refresh()
        if execution.status not in EXECUTION_FINAL_STATUSES:
            execution.cancel()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1500)
def test_recovering_a_stalled_transaction_runs_the_failing_function_again(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    functions = register(
        collection_factory, ["ticks", "left", "failing", "after_left", "after_failing"]
    )
    execution = functions["ticks"].trigger()
    try:
        wait_for_status(execution, {STALLED}, STALL_TIMEOUT)
        assert attempts_of(execution, "failing") == 1, describe(execution)

        transaction_holding(execution, "failing").recover()

        def failed_again() -> bool:
            return (
                attempts_of(execution, "failing") >= 2 and execution.status == STALLED
            )

        deadline = time.monotonic() + STALL_TIMEOUT
        while not failed_again():
            assert time.monotonic() < deadline, (
                "the recovered run did not fail again:\n" + describe(execution)
            )
            time.sleep(POLL_SECONDS)
        assert runs_of(transaction_holding(execution, "failing"))["failing"] == FAILED
    finally:
        execution.refresh()
        if execution.status not in EXECUTION_FINAL_STATUSES:
            execution.cancel()


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1500)
@pytest.mark.monkey
@pytest.mark.ep_case01
def test_canceling_an_execution_does_not_half_cancel_a_later_one(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    functions = register(collection_factory, ["ticks", "left", "right"])
    first, second = cancel_one_while_a_later_one_reads_its_state(functions)
    try:
        wait_for_status(first, EXECUTION_FINAL_STATUSES, STALL_TIMEOUT)
        deadline = time.monotonic() + SETTLE_TIMEOUT
        stuck_since = None
        while True:
            second.refresh()
            transformers = transaction_holding(second, "left")
            unsettled = transformers.status in (
                TRANSACTION_RUNNING,
                TRANSACTION_UNEXPECTED,
            )
            if second.status in EXECUTION_FINAL_STATUSES and not unsettled:
                break
            finished = set(runs_of(transformers).values()) <= FINISHED_RUNS
            if unsettled and finished:
                stuck_since = stuck_since or time.monotonic()
                assert time.monotonic() - stuck_since < STUCK_SECONDS, (
                    f"canceling {first.id} left {second.id} half canceled: its "
                    f"transaction {transformers.id} stays {transformers.status} "
                    "although all its function runs finished\n" + describe(second)
                )
            else:
                stuck_since = None
            assert time.monotonic() < deadline, (
                f"{second.id} never settled\n" + describe(second)
            )
            time.sleep(POLL_SECONDS)
    finally:
        cancel_unfinished(first, second)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1500)
@pytest.mark.monkey
@pytest.mark.ep_case02
def test_a_finished_run_beside_a_canceled_one_is_published_or_discarded(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    functions = register(collection_factory, ["ticks", "left", "right"])
    first, second = cancel_one_while_a_later_one_reads_its_state(functions)
    try:
        wait_for_status(first, EXECUTION_FINAL_STATUSES, STALL_TIMEOUT)
        deadline = time.monotonic() + SETTLE_TIMEOUT
        done_since = None
        while True:
            transformers = transaction_holding(second, "right")
            status = runs_of(transformers)["right"]
            if status in (COMMITTED, CANCELED):
                break
            if status == DONE:
                done_since = done_since or time.monotonic()
                assert time.monotonic() - done_since < STUCK_SECONDS, (
                    f"{second.id}'s right finished, but in the {STUCK_SECONDS} s "
                    "since it was neither published (Committed) nor discarded "
                    f"(Canceled); its transaction {transformers.id} holds:\n"
                    + describe(second)
                )
            assert time.monotonic() < deadline, (
                f"{second.id}'s right never finished\n" + describe(second)
            )
            time.sleep(POLL_SECONDS)
    finally:
        cancel_unfinished(first, second)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(1500)
@pytest.mark.monkey
@pytest.mark.ep_case03
def test_a_stalled_execution_stays_stalled_when_another_transaction_is_canceled(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    functions = register(
        collection_factory, ["ticks", "left", "failing", "after_left", "after_failing"]
    )
    execution = functions["ticks"].trigger()
    try:
        wait_for_status(execution, {STALLED}, STALL_TIMEOUT)

        transaction_holding(execution, "after_failing").cancel()

        destinations = transaction_holding(execution, "after_failing")
        deadline = time.monotonic() + STUCK_SECONDS
        while destinations.status != TRANSACTION_CANCELED:
            assert time.monotonic() < deadline, (
                "the destinations transaction was not canceled\n" + describe(execution)
            )
            time.sleep(POLL_SECONDS)
            destinations = transaction_holding(execution, "after_failing")
        time.sleep(POLL_SECONDS)
        execution.refresh()
        assert execution.status == STALLED, (
            f"{execution.id} still waits for its failed transformer to be recovered "
            f"or canceled, so it should be {STALLED}, but it is {execution.status}\n"
            + describe(execution)
        )
    finally:
        cancel_unfinished(execution)


def cancel_one_while_a_later_one_reads_its_state(
    functions: dict[str, Function],
) -> tuple[Execution, Execution]:
    warm_up = functions["ticks"].trigger()
    wait_for_status(warm_up, {FINISHED}, STALL_TIMEOUT)
    first = functions["left"].trigger()
    second = functions["ticks"].trigger()
    first.cancel()
    return first, second


def cancel_unfinished(*executions: Execution) -> None:
    for execution in executions:
        execution.refresh()
        if execution.status not in EXECUTION_FINAL_STATUSES:
            execution.cancel()


@dataclass
class Event:
    second: float
    action: str
    target: str
    result: str

    def text(self) -> str:
        return (
            f"+{self.second:7.1f}s  {self.action:<20} {self.target:<22} {self.result}"
        )


@dataclass
class Timeline:
    started: float = field(default_factory=time.monotonic)
    events: list[Event] = field(default_factory=list)

    def record(self, action: str, target: str, result: str) -> None:
        self.events.append(
            Event(round(time.monotonic() - self.started, 1), action, target, result)
        )

    def touching(self, execution: Execution) -> list[str]:
        ids = {
            execution.id,
            *(transaction.id for transaction in execution.transactions),
        }
        return [event.text() for event in self.events if event.target in ids]


def attempt(timeline: Timeline, action: str, target: str, call) -> None:
    try:
        call()
    except Exception as error:
        timeline.record(action, target, f"rejected: {error}"[:120])
    else:
        timeline.record(action, target, "ok")


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(3600)
def test_cancels_never_leave_a_transaction_running(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
):
    seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
    rng = random.Random(seed)
    functions = register(
        collection_factory, ["ticks", "left", "right", "after_left", "after_right"]
    )
    timeline = Timeline()
    executions: list[Execution] = []
    for _ in range(ROUNDS):
        execution = functions["ticks"].trigger()
        executions.append(execution)
        timeline.record("trigger ticks", execution.id, "ok")
        time.sleep(rng.uniform(0, 4))
        if rng.random() < 0.5:
            target = rng.choice(executions[-RECENT:])
            target.refresh()
            if rng.random() < 0.5 or not target.transactions:
                attempt(timeline, "cancel execution", target.id, target.cancel)
            else:
                transaction = rng.choice(target.transactions)
                attempt(
                    timeline, "cancel transaction", transaction.id, transaction.cancel
                )

    pending = list(executions)
    deadline = time.monotonic() + SETTLE_TIMEOUT
    last_change = time.monotonic()
    while pending and time.monotonic() < deadline:
        for execution in pending:
            execution.refresh()
        still = [e for e in pending if e.status not in EXECUTION_FINAL_STATUSES]
        if len(still) != len(pending):
            last_change = time.monotonic()
        pending = still
        if not pending or time.monotonic() - last_change >= NO_PROGRESS_SECONDS:
            break
        time.sleep(POLL_SECONDS)

    problems = []
    for execution in executions:
        execution.refresh()
        for transaction in execution.transactions:
            runs = runs_of(transaction)
            stuck = transaction.status in (TRANSACTION_RUNNING, TRANSACTION_UNEXPECTED)
            if stuck and set(runs.values()) <= FINISHED_RUNS:
                problems.append((execution, transaction))
    unfinished = [e for e in executions if e.status not in EXECUTION_FINAL_STATUSES]

    if problems or unfinished:
        report = [f"seed {seed}, {ROUNDS} rounds"]
        for execution, transaction in problems:
            report.append(
                f"transaction {transaction.id} stays {transaction.status} although "
                "all its function runs finished:"
            )
            report.append(describe(execution))
            report.append("  operations on this execution:")
            report += [f"    {line}" for line in timeline.touching(execution)]
        for execution in unfinished:
            report.append(f"execution {execution.id} never finished:")
            report.append(describe(execution))
            report += [f"    {line}" for line in timeline.touching(execution)]
        report.append("all operations:")
        report += [f"  {event.text()}" for event in timeline.events]
        pytest.fail("\n".join(report), pytrace=False)
