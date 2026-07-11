#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks connection resilience: `_api` retries transient transport
failures (the intermittent `SSL: UNEXPECTED_EOF_WHILE_READING`) but never
retries an HTTP error, and `ensure_running` starts + waits for a cold SQL
warehouse before statements are submitted."""
from __future__ import annotations

import io
import urllib.error

import pytest

from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems.databricks import connection


class _Resp:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._payload


def _ssl_eof() -> urllib.error.URLError:
    return urllib.error.URLError(
        "[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of "
        "protocol (_ssl.c:1010)"
    )


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(connection.time, "sleep", lambda _s: None)


def test_api_retries_transient_tls_eof_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=60):
        calls["n"] += 1
        if calls["n"] < 3:
            raise _ssl_eof()
        return _Resp(b'{"ok": true}')

    monkeypatch.setattr(connection.urllib.request, "urlopen", fake_urlopen)
    assert connection._api("h", "t", "POST", "/api/2.0/sql/statements", {}) == {
        "ok": True
    }
    assert calls["n"] == 3  # two failures, then the retry succeeds


def test_api_gives_up_after_max_attempts(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=60):
        calls["n"] += 1
        raise _ssl_eof()

    monkeypatch.setattr(connection.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(DatagenError) as exc:
        connection._api("h", "t", "POST", "/api/2.0/sql/statements", {})
    assert calls["n"] == connection._MAX_ATTEMPTS
    assert "unreachable" in str(exc.value)


def test_api_does_not_retry_http_error(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=60):
        calls["n"] += 1
        raise urllib.error.HTTPError(
            "url", 403, "Forbidden", {}, io.BytesIO(b"denied")
        )

    monkeypatch.setattr(connection.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(DatagenError) as exc:
        connection._api("h", "t", "GET", "/api/2.0/sql/warehouses/w")
    assert calls["n"] == 1  # server answered; no retry
    assert "HTTP 403" in str(exc.value)


def test_ensure_running_returns_immediately_when_warm(monkeypatch):
    seen = []

    def fake_api(host, token, method, path, body=None):
        seen.append((method, path))
        return {"state": "RUNNING"}

    monkeypatch.setattr(connection, "_api", fake_api)
    connection.ensure_running("h", "t", "w")
    assert seen == [("GET", "/api/2.0/sql/warehouses/w")]  # no start, no poll


def test_ensure_running_starts_and_waits_for_stopped_warehouse(monkeypatch):
    states = iter(["STOPPED", "STARTING", "RUNNING"])
    seen = []

    def fake_api(host, token, method, path, body=None):
        seen.append((method, path))
        if method == "POST":
            return {}
        return {"state": next(states)}

    monkeypatch.setattr(connection, "_api", fake_api)
    connection.ensure_running("h", "t", "w")
    assert ("POST", "/api/2.0/sql/warehouses/w/start") in seen
    assert seen[-1] == ("GET", "/api/2.0/sql/warehouses/w")


def test_ensure_running_times_out(monkeypatch):
    def fake_api(host, token, method, path, body=None):
        return {} if method == "POST" else {"state": "STARTING"}

    monkeypatch.setattr(connection, "_api", fake_api)
    monkeypatch.setattr(connection, "_READY_TIMEOUT", 10)
    monkeypatch.setattr(connection, "_READY_POLL", 5)
    with pytest.raises(DatagenError) as exc:
        connection.ensure_running("h", "t", "w")
    assert "not RUNNING" in str(exc.value)
