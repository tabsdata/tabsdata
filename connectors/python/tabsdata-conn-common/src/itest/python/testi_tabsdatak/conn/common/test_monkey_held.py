#
# Copyright 2026 Tabsdata Inc.
#

"""Transactions whose runs are all Done but wait to commit behind a stalled
transaction they read from."""

from __future__ import annotations

from types import SimpleNamespace

from testi_tabsdatak.conn.common.monkey.held import held_transactions

UPSTREAM = {"tfr04": "pub15", "tfr06": "pub15", "sub01": "tfr06"}


def tx(id: str, status: str):
    return SimpleNamespace(id=id, status=status)


def run(function: str, status: str):
    return SimpleNamespace(function=function, status=status)


def held(transactions: list, runs: dict[str, list]) -> set[str]:
    return held_transactions(transactions, runs, UPSTREAM.get)


def test_a_done_run_reading_its_own_output_held_by_a_stalled_transaction_waits():
    transactions = [tx("t1", "Stalled"), tx("t2", "Running"), tx("t3", "Running")]
    runs = {
        "t1": [run("tfr04", "On Hold"), run("tfr06", "Done")],
        "t2": [run("tfr06", "Done")],
        "t3": [run("sub01", "Done")],
    }
    assert held(transactions, runs) == {"t2", "t3"}


def test_a_transaction_with_an_unfinished_run_is_not_held():
    transactions = [tx("t1", "Stalled"), tx("t2", "Running")]
    runs = {
        "t1": [run("tfr06", "Done")],
        "t2": [run("tfr06", "Done"), run("sub01", "Running")],
    }
    assert held(transactions, runs) == set()


def test_a_done_transaction_reading_nothing_stalled_is_not_held():
    transactions = [tx("t1", "Stalled"), tx("t2", "Running")]
    runs = {
        "t1": [run("tfr04", "Failed")],
        "t2": [run("tfr06", "Done")],
    }
    assert held(transactions, runs) == set()
