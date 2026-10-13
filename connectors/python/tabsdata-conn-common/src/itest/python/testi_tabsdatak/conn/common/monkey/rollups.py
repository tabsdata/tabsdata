#
# Copyright 2026 Tabsdata Inc.
#

"""What a settled execution must show through the API about its parts: an
end time exactly once its parts end, a span covering them (exactly, for an
execution's transactions; at least, for a transaction's runs, which show only
their current attempt), and no live function worker."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from tabsdatak.sdk.status_utils.function_run import (
    FunctionRunStatus,
    function_run_status_to_mapping,
)
from tabsdatak.sdk.status_utils.transaction import (
    TRANSACTION_SUCCESSFUL_FINAL_STATUSES,
)
from tabsdatak.sdk.status_utils.worker import WorkerStatus, worker_status_to_mapping

RUN_TERMINALS = {
    function_run_status_to_mapping(status.value)
    for status in (
        FunctionRunStatus.COMMITTED,
        FunctionRunStatus.CANCELED,
        FunctionRunStatus.YANKED,
    )
}
TRANSACTION_TERMINALS = TRANSACTION_SUCCESSFUL_FINAL_STATUSES
LIVE_WORKERS = {
    worker_status_to_mapping(status.value)
    for status in (
        WorkerStatus.SCHEDULED,
        WorkerStatus.RUN_REQUESTED,
        WorkerStatus.RUNNING,
    )
}
FUNCTION_WORKER = "F"


NO_END = "Ended with no end time"
EARLY_END = "End time while parts are still going"
LATE_START = "Started after its first part"
EARLY_END_TIME = "Ended before its last part"
SPAN = "Span differs from its transactions"
LIVE_WORKER = "Live function worker of a settled execution"


@dataclass(frozen=True)
class Problem:
    kind: str
    text: str


def started(parts: Iterable) -> list:
    return [part.started_on for part in parts if part.started_on is not None]


def ended(parts: Iterable) -> list:
    return [part.ended_on for part in parts if part.ended_on is not None]


def end_problem(
    name: str, parent, children: list, terminals: set[str]
) -> list[Problem]:
    if not children:
        return []
    finished = all(child.status in terminals for child in children)
    if finished and parent.ended_on is None:
        return [
            Problem(
                NO_END,
                f"{name} is {parent.status} with all its parts ended, but has no end",
            )
        ]
    if not finished and parent.ended_on is not None:
        unended = sorted({child.status for child in children} - terminals)
        return [
            Problem(
                EARLY_END,
                f"{name} ended at {parent.ended_on} although parts of it are still "
                f"{', '.join(unended)}",
            )
        ]
    return []


def transaction_problems(transaction, runs: list) -> list[Problem]:
    name = f"Transaction {transaction.id} of execution {transaction.execution.id}"
    problems = end_problem(name, transaction, runs, RUN_TERMINALS)
    starts, ends = started(runs), ended(runs)
    if starts and (
        transaction.started_on is None or transaction.started_on > min(starts)
    ):
        problems.append(
            Problem(
                LATE_START,
                f"{name} started at {transaction.started_on}, after its first function "
                f"run started at {min(starts)}",
            )
        )
    if transaction.ended_on is not None and ends and transaction.ended_on < max(ends):
        problems.append(
            Problem(
                EARLY_END_TIME,
                f"{name} ended at {transaction.ended_on}, before its last function run "
                f"ended at {max(ends)}",
            )
        )
    return problems


def execution_problems(execution, transactions: list) -> list[Problem]:
    name = f"Execution {execution.id}"
    problems = end_problem(name, execution, transactions, TRANSACTION_TERMINALS)
    starts = started(transactions)
    first = min(starts) if starts else None
    if execution.started_on != first:
        problems.append(
            Problem(
                SPAN,
                f"{name} started at {execution.started_on}, but its first transaction "
                f"started at {first}",
            )
        )
    ends = ended(transactions)
    if execution.ended_on is not None and execution.ended_on != max(ends, default=None):
        problems.append(
            Problem(
                SPAN,
                f"{name} ended at {execution.ended_on}, but its last transaction ended "
                f"at {max(ends, default=None)}",
            )
        )
    return problems


def worker_problems(execution, workers: list) -> list[Problem]:
    return [
        Problem(
            LIVE_WORKER,
            f"Worker {worker.id} of execution {execution.id} is still {worker.status} "
            f"although the execution is {execution.status}",
        )
        for worker in workers
        if worker.type == FUNCTION_WORKER and worker.status in LIVE_WORKERS
    ]
