#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

# noinspection PyProtectedMember
from tabsdatak.sdk import tabsdata_server as ts


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(ts, "time", fake)
    return fake


def _connection() -> MagicMock:
    connection = MagicMock()
    connection.timeout = (10, 60)
    return connection


def _listing(rows: list[dict]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"data": {"data": rows}}
    return response


def _execution(statuses: list[str]) -> ts.Execution:
    connection = _connection()
    connection.execution_list.side_effect = [
        _listing([{"id": "P1", "status": status}]) for status in statuses
    ]
    return ts.Execution(connection, "P1")


def _function_run(rows: list[dict], monkeypatch) -> ts.FunctionRun:
    connection = _connection()
    runs = iter(rows)

    def _list(self, filter=None, **_):
        return [ts.FunctionRun(self.connection, "FR1", **next(runs))]

    monkeypatch.setattr(ts.TabsdataServer, "list_function_runs", _list)
    return ts.FunctionRun(connection, "FR1", status="R")


def test_final_on_the_first_poll_does_not_sleep(clock):
    execution = _execution(["F"])
    assert execution.wait(timeout=30) is execution
    assert execution.status == "Finished"
    assert clock.sleeps == []


def test_polls_back_off_to_the_poll_interval(clock):
    execution = _execution(["S", "R", "R", "R", "R", "F"])
    execution.wait(poll_interval=5)
    assert clock.sleeps == [1, 2, 4, 5, 5]


def test_timeout_raises_with_the_last_object(clock):
    execution = _execution(["R"] * 10)
    with pytest.raises(ts.WaitTimeout) as raised:
        execution.wait(timeout=3.5)
    assert raised.value.last is execution
    assert raised.value.timeout == 3.5
    assert sum(clock.sleeps) == 3.5


def test_read_timeout_follows_the_remaining_budget(clock):
    execution = _execution([])
    connection = execution.connection
    statuses = iter(["R", "R", "F"])
    seen = []

    def _respond(*_, **__):
        seen.append(connection.timeout)
        return _listing([{"id": "P1", "status": next(statuses)}])

    connection.execution_list.side_effect = _respond
    execution.wait(timeout=2.5)
    assert seen == [(10, 2.5), (10, 1.5), (10, 1.0)]
    assert connection.timeout == (10, 60)


def test_a_read_timeout_inside_the_budget_is_a_wait_timeout(clock):
    execution = _execution([])
    execution.connection.execution_list.side_effect = requests.exceptions.ReadTimeout
    with pytest.raises(ts.WaitTimeout):
        execution.wait(timeout=5)


def test_a_read_timeout_without_a_budget_propagates(clock):
    execution = _execution([])
    execution.connection.execution_list.side_effect = requests.exceptions.ReadTimeout
    with pytest.raises(requests.exceptions.ReadTimeout):
        execution.wait()


def test_on_update_sees_every_refresh(clock):
    execution = _execution(["S", "R", "F"])
    statuses = []
    execution.wait(on_update=lambda e: statuses.append(e.status))
    assert statuses == ["Scheduled", "Running", "Finished"]


def test_transaction_waits_for_a_final_status(clock, monkeypatch):
    rows = iter([{"status": "R"}, {"status": "C"}])

    def _list(self, filter=None, **_):
        return [ts.Transaction(self.connection, "T1", **next(rows))]

    monkeypatch.setattr(ts.TabsdataServer, "list_transactions", _list)
    transaction = ts.Transaction(_connection(), "T1", status="S")
    assert transaction.wait().status == "Committed"


def test_function_run_done_is_not_final(clock, monkeypatch):
    run = _function_run([{"status": "D"}, {"status": "C"}], monkeypatch)
    assert run.wait().status == "Committed"
    assert clock.sleeps == [1]


def test_function_run_error_can_run_again(clock, monkeypatch):
    run = _function_run(
        [{"status": "E"}, {"status": "R"}, {"status": "C"}], monkeypatch
    )
    assert run.wait().status == "Committed"


def test_attempt_waits_on_the_physical_status(clock, monkeypatch):
    run = _function_run(
        [
            {"status": "F", "physical_function_run_id": "A1", "physical_status": "F"},
            {"status": "F", "physical_function_run_id": "A2", "physical_status": "R"},
            {"status": "F", "physical_function_run_id": "A2", "physical_status": "E"},
        ],
        monkeypatch,
    )
    run.wait(attempt="A2")
    assert run._data["physical_status"] == "E"
    assert clock.sleeps == [1, 2]


def test_attempt_that_never_becomes_current_times_out(clock, monkeypatch):
    rows = [
        {"status": "F", "physical_function_run_id": "A1", "physical_status": "F"}
    ] * 5
    run = _function_run(rows, monkeypatch)
    with pytest.raises(ts.WaitTimeout):
        run.wait(timeout=2, attempt="A2")


def test_a_superseded_attempt_ends_the_wait(clock, monkeypatch):
    rows = [
        {"status": "F", "physical_function_run_id": "A2", "physical_status": "R"},
        {"status": "F", "physical_function_run_id": "A3", "physical_status": "R"},
    ]
    run = _function_run(rows, monkeypatch)
    with pytest.raises(ts.AttemptSuperseded) as raised:
        run.wait(attempt="A2")
    assert raised.value.current == "A3"
    assert clock.sleeps == [1]


def test_logical_attempt_ids_order_by_their_counter(clock, monkeypatch):
    rows = [
        {
            "status": "F",
            "physical_function_run_id": "pfr20260930-9",
            "physical_status": "R",
        },
        {
            "status": "F",
            "physical_function_run_id": "pfr20260930-10",
            "physical_status": "R",
        },
    ]
    run = _function_run(rows, monkeypatch)
    with pytest.raises(ts.AttemptSuperseded) as raised:
        run.wait(attempt="pfr20260930-0000000009")
    assert raised.value.current == "pfr20260930-10"


def test_a_padded_attempt_id_matches_its_unpadded_form(clock, monkeypatch):
    rows = [
        {
            "status": "F",
            "physical_function_run_id": "pfr20260930-2",
            "physical_status": "D",
        }
    ]
    run = _function_run(rows, monkeypatch)
    run.wait(attempt="pfr20260930-0000000002")
    assert run._data["physical_status"] == "D"


def _fetcher(*responses):
    calls = iter(responses)

    def fetch():
        answer = next(calls)
        if isinstance(answer, Exception):
            raise answer
        return answer

    return fetch


def _poll(fetch, **kwargs):
    return ts.poll_until(
        fetch,
        lambda items: all(item == "done" for item in items),
        connection=_connection(),
        **kwargs,
    )


def test_a_wait_rides_out_a_short_outage(clock):
    reset = requests.exceptions.ConnectionError("Connection reset by peer")
    fetch = _fetcher(
        ["running"],
        reset,
        requests.exceptions.ReadTimeout("read timed out"),
        reset,
        ["done"],
    )
    assert _poll(fetch, outage_grace=30) == ["done"]


def test_a_wait_gives_up_after_the_outage_grace(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    fetch = _fetcher(["running"], *[down] * 20)
    with pytest.raises(requests.exceptions.ConnectionError):
        _poll(fetch, outage_grace=5, poll_interval=1)
    assert clock.now > 5


def test_the_outage_grace_restarts_after_the_server_answers(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    fetch = _fetcher(
        *[down] * 4, ["running"], *[down] * 4, ["running"], *[down] * 4, ["done"]
    )
    assert _poll(fetch, outage_grace=5, poll_interval=1) == ["done"]


def test_an_outage_is_reported_when_it_starts_and_ends(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    seen = []
    fetch = _fetcher(["running"], down, down, ["done"])
    _poll(fetch, on_outage=seen.append)
    assert seen == [down, None]


def test_the_wait_timeout_wins_over_an_outage(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    fetch = _fetcher(["running"], *[down] * 20)
    with pytest.raises(ts.WaitTimeout) as raised:
        _poll(fetch, timeout=4)
    assert raised.value.last == ["running"]


def test_a_wait_does_not_swallow_other_errors(clock):
    with pytest.raises(ValueError):
        _poll(_fetcher(ValueError("bad payload")))
