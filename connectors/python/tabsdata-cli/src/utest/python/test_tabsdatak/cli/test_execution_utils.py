#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import requests
from rich.table import Table

from tabsdatak.cli.td import execution_utils


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


def _monitor(fetch, **kwargs):
    return execution_utils.live_monitor(
        ["running"],
        fetch,
        lambda items: Table(*items),
        lambda item: item == "done",
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
