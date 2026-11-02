#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.sdk import _apiserver as a, tabsdata_server as ts


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


def _response(data) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"data": data}
    return response


def _server(read_timeout: float = 60) -> ts.TabsdataServer:
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = MagicMock()
    server.connection.timeout = (10, read_timeout)
    return server


def _probes(server, clock, statuses, hold=0.0):
    waits = []
    statuses = iter(statuses)

    def _get(probe_id, wait=None):
        waits.append(wait)
        clock.now += wait if hold is None else min(wait, hold)
        return _response({"probe_id": probe_id, "status": next(statuses)})

    server.connection.connection_probe_get.side_effect = _get
    return waits


def test_wait_long_polls_until_the_probe_ends(clock):
    server = _server()
    waits = _probes(server, clock, ["running", "succeeded"], hold=None)
    seen = []
    probe = server.wait_connection_probe("PR1", timeout=100, on_update=seen.append)
    assert probe["status"] == "succeeded"
    assert waits == [55, 45]
    assert [p["status"] for p in seen] == ["running", "succeeded"]
    assert clock.sleeps == []


def test_wait_stays_under_the_read_timeout(clock):
    server = _server(read_timeout=30)
    waits = _probes(server, clock, ["queued", "failed"], hold=None)
    assert server.wait_connection_probe("PR1")["status"] == "failed"
    assert waits == [25, 25]


def test_wait_raises_with_the_last_probe_when_the_budget_runs_out(clock):
    server = _server()
    _probes(server, clock, ["queued", "running", "running"], hold=None)
    with pytest.raises(ts.WaitTimeout) as raised:
        server.wait_connection_probe("PR1", timeout=70)
    assert raised.value.last["status"] == "running"
    assert raised.value.timeout == 70


def test_a_probe_ended_on_submission_needs_no_long_poll(clock):
    server = _server()
    waits = _probes(server, clock, ["cancelled"])
    assert server.wait_connection_probe("PR1", timeout=10)["status"] == "cancelled"
    assert waits == [10]


def test_a_zero_wait_sleeps_between_reads(clock):
    server = _server(read_timeout=3)
    waits = _probes(server, clock, ["running", "succeeded"])
    server.wait_connection_probe("PR1")
    assert waits == [0, 0]
    assert clock.sleeps == [ts.WAIT_FIRST_INTERVAL]


class _Conn:
    pass


class _OtherConn:
    pass


class _PgSrc:
    pass


class _PgCdc:
    pass


class _PgDest:
    pass


def _def(type_, conn, **api):
    return SimpleNamespace(type_=type_, conn=conn, **api)


@pytest.fixture
def registry(monkeypatch):
    # noinspection PyProtectedMember
    from tabsdatak import _connectors

    monkeypatch.setattr(
        _connectors,
        "avail_srcs",
        lambda: {
            "pg-in": _def("pg-in", _Conn, src=_PgSrc),
            "other-in": _def("other-in", _OtherConn, src=_PgSrc),
        },
    )
    monkeypatch.setattr(
        _connectors,
        "avail_stream_srcs",
        lambda: {"pg-cdc": _def("pg-cdc", _Conn, src=_PgCdc)},
    )
    monkeypatch.setattr(
        _connectors,
        "avail_dests",
        lambda: {"pg-out": _def("pg-out", _Conn, dest=_PgDest)},
    )
    return f"{_Conn.__module__}:{_Conn.__qualname__}"


def test_compatibility_comes_from_the_local_connectors(registry):
    assert ts.connection_compatibility(registry) == {
        "types": ["pg-in", "pg-cdc", "pg-out"],
        "publishers": ["_PgSrc", "_PgCdc"],
        "subscribers": ["_PgDest"],
    }


def test_an_unknown_kind_is_compatible_with_nothing(registry):
    assert ts.connection_compatibility("x:Nope") == {
        "types": [],
        "publishers": [],
        "subscribers": [],
    }


def test_listed_connections_and_the_catalog_carry_their_compatibility(registry):
    server = _server()
    server.connection.connection_list.return_value = _response({
        "data": [{"collection": "crm", "kind": registry}]
    })
    server.connection.connection_catalog_get.return_value = _response({
        "collection": "crm",
        "kind": "x:Nope",
    })
    [row] = list(server.list_connections_generator(filter=["check_status:eq:ok"]))
    assert row["compatible"]["types"] == ["pg-in", "pg-cdc", "pg-out"]
    assert server.connection.connection_list.call_args.kwargs["request_filter"] == [
        "check_status:eq:ok"
    ]
    assert server.get_connection_catalog("crm")["compatible"]["types"] == []


def _api(project: str | None = "p1") -> a.APIServer:
    api = a.APIServer("http://h:1")
    api.project = project
    api._active_project_name_for = project
    api._resolved_active_project_name = project
    api.get = MagicMock(return_value=MagicMock(status_code=200))
    api.post = MagicMock(return_value=MagicMock(status_code=202))
    api.put = MagicMock(return_value=MagicMock(status_code=200))
    api.delete = MagicMock(return_value=MagicMock(status_code=200))
    return api


def test_the_connection_list_narrows_to_the_active_project():
    api = _api()
    api.connection_list(request_filter=["kind:eq:m:C"], raise_for_status=False)
    path, params = api.get.call_args.args
    assert path == "/connections"
    assert params == {"filter": ["kind:eq:m:C", "project:eq:p1"]}
    api = _api(project=None)
    api.connection_list(raise_for_status=False)
    assert api.get.call_args.args == ("/connections", {})


def test_the_catalog_routes_are_scoped_to_the_project():
    api = _api()
    api.connection_catalog_get("crm", raise_for_status=False)
    assert api.get.call_args.args == (
        "/projects/p1/collections/crm/connection/catalog",
    )
    api.connection_entity_list("crm", fields=True, request_len=5, raise_for_status=False)
    assert api.get.call_args.args == (
        "/projects/p1/collections/crm/connection/entities",
        {"len": 5, "fields": "true"},
    )
    api.connection_probe_get("PR1", wait=55, raise_for_status=False)
    assert api.get.call_args.args == (
        "/projects/p1/connection-probes/PR1",
        {"wait": 55},
    )
    with pytest.raises(a.NoProjectSelectedError):
        _api(project=None).connection_probe_get("PR1")


def test_probe_submissions_send_only_the_given_params():
    api = _api()
    api.connection_probe_submit("crm", "check", raise_for_status=False)
    assert api.post.call_args.args == ("/projects/p1/collections/crm/connection/probes",)
    assert api.post.call_args.kwargs == {"json": {"kind": "check"}}
    api.connection_probe_submit(
        "crm", "sample", entity="db.t", limit=5, raise_for_status=False
    )
    assert api.post.call_args.kwargs == {
        "json": {"kind": "sample", "entity": "db.t", "limit": 5}
    }
    api.connection_probe_cancel("PR1", raise_for_status=False)
    assert api.post.call_args.args == ("/projects/p1/connection-probes/PR1/cancel",)


def test_a_rate_limited_submission_raises_with_its_status():
    server = _server()
    response = MagicMock(status_code=429, headers={})
    response.json.return_value = {"error_description": "rate_limited: retry after 9s"}
    server.connection.connection_probe_submit.side_effect = (
        a.APIServerError.from_response(response)
    )
    with pytest.raises(a.APIServerError) as raised:
        server.submit_connection_probe("crm", "check")
    assert raised.value.status == 429
