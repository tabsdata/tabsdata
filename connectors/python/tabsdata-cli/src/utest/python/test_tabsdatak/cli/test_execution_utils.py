#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from tabsdatak.cli.td import execution_utils
from tabsdatak.common._tabular import Column, Kind, TableView


@pytest.fixture
def clock(monkeypatch):
    now = [0.0]

    def fake_sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(execution_utils, "sleep", fake_sleep)
    monkeypatch.setattr(execution_utils, "monotonic", lambda: now[0])
    return now


def _fetcher(*responses):
    calls = iter(responses)

    def fetch():
        answer = next(calls)
        if isinstance(answer, Exception):
            raise answer
        return answer

    return fetch


def _view(items):
    return TableView(title="Items", columns=[Column("Status")], rows=[[i] for i in items])


def _monitor(fetch, **kwargs):
    return execution_utils.live_monitor(
        ["running"],
        fetch,
        _view,
        lambda items: all(item == "done" for item in items),
        **kwargs,
    )


def test_live_monitor_polls_until_every_item_is_final(clock):
    fetch = _fetcher(["running"], ["running", "done"], ["done", "done"])
    assert _monitor(fetch) == ["done", "done"]


def test_live_monitor_rides_out_a_short_outage(clock):
    reset = requests.exceptions.ConnectionError("Connection reset by peer")
    fetch = _fetcher(
        ["running"],
        reset,
        requests.exceptions.ReadTimeout("read timed out"),
        reset,
        ["done"],
    )
    assert _monitor(fetch, outage_grace=10) == ["done"]


def test_live_monitor_gives_up_after_the_grace_period(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    fetch = _fetcher(["running"], *[down] * 20)
    with pytest.raises(requests.exceptions.ConnectionError):
        _monitor(fetch, outage_grace=5)
    assert clock[0] > 5


def test_live_monitor_grace_restarts_after_the_server_answers(clock):
    down = requests.exceptions.ConnectionError("Connection refused")
    fetch = _fetcher(
        *[down] * 4, ["running"], *[down] * 4, ["running"], *[down] * 4, ["done"]
    )
    assert _monitor(fetch, outage_grace=5) == ["done"]


def test_live_monitor_does_not_swallow_other_errors(clock):
    fetch = _fetcher(ValueError("bad payload"))
    with pytest.raises(ValueError):
        _monitor(fetch)


def test_live_monitor_shows_how_long_it_has_been_watching(clock):
    frames = []

    def build(items):
        view = _view(items)
        frames.append(view)
        return view

    fetch = _fetcher(["running"], ["running"], ["done"])
    execution_utils.live_monitor(["running"], fetch, build, lambda items: items == ["done"])
    assert frames[0].context == ["watching for 0s", "refreshing every 1s"]
    assert frames[2].context == ["watching for 1s", "refreshing every 1s"]
    assert frames[-1].context == ["watched for 2s"]


def test_live_monitor_keeps_the_notes_of_the_listing(clock):
    frames = []

    def build(items):
        view = _view(items)
        frames.append(view)
        return view

    execution_utils.live_monitor(
        ["done"], _fetcher(["done"]), build, lambda _: True, notes=["More results."]
    )
    assert frames[-1].notes == ["More results."]


@pytest.mark.parametrize(
    "seconds, label",
    [(0, "0s"), (59.9, "59s"), (61, "1m 01s"), (3600, "1h 00m 00s"), (3725, "1h 02m 05s")],
)
def test_elapsed_label(seconds, label):
    assert execution_utils.elapsed_label(seconds) == label


def _row(**kwargs):
    defaults = dict(
        id="ID1",
        name="a_plan_with_a_rather_long_name",
        project=None,
        collection=SimpleNamespace(name="input"),
        function=SimpleNamespace(name="pub"),
        triggered_by="admin",
        started_on_str="2026-10-22 10:00:00",
        ended_on_str="None",
        status="Failed",
        type="ST",
        kwargs={"retry_at": None},
        _data={},
    )
    return SimpleNamespace(**{**defaults, **kwargs})


def test_plans_view_keeps_missing_values_for_the_placeholder():
    view = execution_utils.plans_view([_row()])
    assert view.count == "plans"
    assert view.rows == [
        [
            "ID1",
            "a_plan_with_a_rather_long_name",
            None,
            "input / pub",
            "admin",
            "2026-10-22 10:00:00",
            None,
            "Failed",
        ]
    ]
    assert [c.kind for c in view.columns][-1] is Kind.STATUS
    assert view.columns[1].width == 20


def test_indexed_workers_view_numbers_its_rows():
    view = execution_utils.workers_view([_row(), _row(id="ID2")], indexed=True)
    assert view.columns[0] == Column("Index", Kind.NUMBER)
    assert [row[:2] for row in view.rows] == [[1, "ID1"], [2, "ID2"]]
    assert view.rows[0][-1] == "Failed (needs action)"
