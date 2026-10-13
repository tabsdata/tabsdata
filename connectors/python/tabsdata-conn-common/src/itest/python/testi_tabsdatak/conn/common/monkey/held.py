#
# Copyright 2026 Tabsdata Inc.
#

"""Transactions held behind a stalled one. A transaction commits only once
every version it read is committed, and a run may read a Done version of a
transaction that later stalls. Its runs all end Done, yet it stays Running
until the stalled transaction is recovered or canceled: an expected end state,
like Stalled. A transformer reads its upstream table and its own previous
output; a subscriber reads its upstream table."""

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
RUNNING = transaction_status_to_mapping(TransactionStatus.RUNNING.value)
STALLED = transaction_status_to_mapping(TransactionStatus.STALLED.value)


def held_transactions(
    transactions: Iterable,
    runs: Mapping[str, list],
    upstream: Callable[[str], str | None],
) -> set[str]:
    """Running transactions whose runs are all Done and read, directly or
    through other held ones, a function with a Done run in a stalled
    transaction."""
    transactions = list(transactions)
    blocking = {tx.id for tx in transactions if tx.status == STALLED}
    waiting = {
        tx.id
        for tx in transactions
        if tx.status == RUNNING
        and runs[tx.id]
        and all(run.status == DONE for run in runs[tx.id])
    }
    held: set[str] = set()
    while True:
        sources = {
            run.function
            for tx in blocking | held
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
