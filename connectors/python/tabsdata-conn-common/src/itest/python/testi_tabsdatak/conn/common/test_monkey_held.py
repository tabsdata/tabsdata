#
# Copyright 2026 Tabsdata Inc.
#

"""Transactions whose runs are all Done but wait to commit behind a stalled
transaction they read from."""

from __future__ import annotations

from types import SimpleNamespace

from testi_tabsdatak.conn.common.monkey.held import held_transactions

UPSTREAM = {"tfr04": "pub15", "tfr06": "pub15", "sub01": "tfr06", "sub02": "tfr06"}


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


def test_a_transaction_queued_behind_a_failed_run_holds_its_readers():
    transactions = [
        tx("t1", "Stalled"),
        tx("t2", "Running"),
        tx("t3", "Running"),
        tx("t4", "Running"),
    ]
    runs = {
        "t1": [run("tfr04", "Failed")],
        "t2": [run("tfr04", "Scheduled"), run("tfr06", "Done")],
        "t3": [run("sub01", "Done"), run("sub02", "Done")],
        "t4": [run("sub02", "Done")],
    }
    assert held(transactions, runs) == {"t2", "t3", "t4"}


def test_runs_naming_their_function_as_the_server_does_are_held_through_upstreams():
    def served(function: str, status: str):
        return SimpleNamespace(
            function=SimpleNamespace(name=function, collection="c"), status=status
        )

    transactions = [
        tx("t1", "Stalled"),
        tx("t2", "Running"),
        tx("t3", "Running"),
    ]
    runs = {
        "t1": [served("tfr04", "Failed")],
        "t2": [served("tfr04", "Scheduled"), served("tfr06", "Done")],
        "t3": [served("sub01", "Done")],
    }
    assert held(transactions, runs) == {"t2", "t3"}


def test_a_transaction_scheduled_behind_nothing_stalled_is_not_held():
    transactions = [tx("t1", "Stalled"), tx("t2", "Running"), tx("t3", "Running")]
    runs = {
        "t1": [run("tfr04", "Failed")],
        "t2": [run("tfr06", "Scheduled")],
        "t3": [run("sub01", "Done")],
    }
    assert held(transactions, runs) == set()


def test_a_done_transaction_reading_nothing_stalled_is_not_held():
    transactions = [tx("t1", "Stalled"), tx("t2", "Running")]
    runs = {
        "t1": [run("tfr04", "Failed")],
        "t2": [run("tfr06", "Done")],
    }
    assert held(transactions, runs) == set()
