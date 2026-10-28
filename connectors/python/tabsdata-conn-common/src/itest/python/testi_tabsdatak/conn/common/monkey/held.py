#
# Copyright 2026 Tabsdata Inc.
#

"""Transactions held behind a stalled one. A transaction commits only once
every version it read is committed, and a run may read a Done version of a
transaction that later stalls. Its runs all end Done, yet it stays Running
until the stalled transaction is recovered or canceled: an expected end state,
like Stalled. A transaction stalls likewise when one of its runs is Scheduled
behind a Failed or On Hold run of its function: it stays Running, never to
commit. A transformer reads its upstream table and its own previous output; a
subscriber reads its upstream table."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from tabsdatak.sdk.status_utils.function_run import (
    FunctionRunStatus,
    function_run_status_to_mapping,
)
from tabsdatak.sdk.status_utils.transaction import (
    TransactionStatus,
    transaction_status_to_mapping,
)

DONE = function_run_status_to_mapping(FunctionRunStatus.DONE.value)
SCHEDULED = function_run_status_to_mapping(FunctionRunStatus.SCHEDULED.value)
BLOCKED = {
    function_run_status_to_mapping(FunctionRunStatus.FAILED.value),
    function_run_status_to_mapping(FunctionRunStatus.ON_HOLD.value),
}
RUNNING = transaction_status_to_mapping(TransactionStatus.RUNNING.value)
STALLED = transaction_status_to_mapping(TransactionStatus.STALLED.value)


def held_transactions(
    transactions: Iterable,
    runs: Mapping[str, list],
    upstream: Callable[[str], str | None],
) -> set[str]:
    """Running transactions queued behind a stalled one, and those whose runs
    are all Done and read, directly or through other held ones, a function
    with a Done run in a stalled or held transaction."""
    transactions = list(transactions)
    stalled = {tx.id for tx in transactions if tx.status == STALLED}
    blocked = {
        run.function for tx in stalled for run in runs[tx] if run.status in BLOCKED
    }
    queued = {
        tx.id
        for tx in transactions
        if tx.status == RUNNING
        and any(
            run.status == SCHEDULED and run.function in blocked for run in runs[tx.id]
        )
    }
    waiting = {
        tx.id
        for tx in transactions
        if tx.status == RUNNING
        and runs[tx.id]
        and all(run.status == DONE for run in runs[tx.id])
    }
    held = set(queued)
    while True:
        sources = {
            run.function
            for tx in stalled | held
            for run in runs[tx]
            if run.status == DONE
        }
        reached = {
            tx
            for tx in waiting - held
            if any(
                run.function in sources or upstream(run.function) in sources
                for run in runs[tx]
            )
        }
        if not reached:
            return held
        held |= reached
