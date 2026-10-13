#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the low-level apiserver HTTP wrapper.

Network-touching code paths (`APIServer.get` / `post` / etc.) are
exercised end-to-end in the legacy integration test suite. Here we
test the pure-Python helpers: URL processing, error wrapping, the
auth-header property, the refresh-trigger gate, and the small
factory helpers."""

from __future__ import annotations

import http.server
import threading
import time

import pytest

# noinspection PyProtectedMember
from tabsdatak.sdk import _apiserver as a

# ---------------------------------------------------------------------------
# process_url
# ---------------------------------------------------------------------------


def test_process_url_adds_http_protocol_when_missing():
    assert a.process_url("host").startswith("http://")
    assert a.process_url("host:1234").startswith("http://")


def test_process_url_preserves_https_protocol():
    out = a.process_url("https://host")
    assert out.startswith("https://")
    assert "://host" in out


def test_process_url_appends_default_port_when_missing():
    out = a.process_url("http://host")
    assert f":{a.DEFAULT_APISERVER_PORT}" in out


def test_process_url_appends_base_api_url():
    out = a.process_url("http://host:1234")
    assert out.endswith(a.BASE_API_URL)


def test_process_url_idempotent_on_already_processed_url():
    once = a.process_url("host")
    twice = a.process_url(once)
    assert once == twice


# ---------------------------------------------------------------------------
# APIServerError
# ---------------------------------------------------------------------------


def test_apiserver_error_extracts_fields_from_dict():
    e = a.APIServerError({
        "code": "X-1",
        "error": "boom",
        "error_description": "the engine exploded",
    })
    assert e.code == "X-1"
    assert e.error == "boom"
    assert e.error_description == "the engine exploded"
    assert "the engine exploded" in str(e)


def test_apiserver_error_falls_back_to_unknown_when_description_missing():
    e = a.APIServerError({"code": "X-1"})
    assert e.error_description is None
    assert "Unknown error" in str(e)


def test_apiserver_error_wraps_non_dict_input_as_description():
    e = a.APIServerError("plain text reason")
    assert e.error_description == "plain text reason"


def test_raise_for_status_falls_back_to_text_on_non_json_body():
    import requests

    class _Resp:
        text = "expected: Collection name, a [A-Za-z0-9] word"

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("400")

        def json(self):
            raise ValueError("not json")

    with pytest.raises(a.APIServerError) as excinfo:
        a.APIServer.raise_for_status(_Resp())
    assert "Collection name" in str(excinfo.value)


# ---------------------------------------------------------------------------
# _obtain_certificate_file_path
# ---------------------------------------------------------------------------


def test_obtain_certificate_file_path_uses_host_port_pattern():
    path = a._obtain_certificate_file_path("https://my.host.example:8443/api/v1")
    assert path.endswith("my_host_example_8443_cert.pem")
    assert a.DEFAULT_TABSDATA_CERTIFICATE_FOLDER in path


def test_obtain_certificate_file_path_falls_back_to_default_port():
    path = a._obtain_certificate_file_path("https://host")
    assert f"_{a.DEFAULT_APISERVER_PORT}_cert.pem" in path


def test_obtain_certificate_file_path_raises_on_malformed_input():
    with pytest.raises(ValueError):
        a._obtain_certificate_file_path(None)


# ---------------------------------------------------------------------------
# APIServer behaviour (no I/O)
# ---------------------------------------------------------------------------


def test_apiserver_normalizes_url_at_construction():
    s = a.APIServer("host")
    assert s.url.startswith("http://")
    assert s.url.endswith(a.BASE_API_URL)


def test_apiserver_authentication_header_empty_without_token():
    s = a.APIServer("host")
    assert s.authentication_header == {}


def test_apiserver_authentication_header_includes_bearer_when_set():
    s = a.APIServer("host")
    s.bearer_token = "abc123"
    assert s.authentication_header == {"Authorization": "Bearer abc123"}


def test_apiserver_certificate_file_is_none_when_not_https():
    s = a.APIServer("host")
    assert s.certificate_file is None


def test_apiserver_refresh_skipped_when_no_refresh_token():
    s = a.APIServer("host")
    s.refresh_token = None
    s.expiration_time = time.time() - 100  # already expired
    # _refresh_token_if_needed must short-circuit when refresh_token is None.
    s._refresh_token_if_needed()  # no exception, no I/O


def test_apiserver_refresh_skipped_when_expiration_time_far_future():
    s = a.APIServer("host")
    s.refresh_token = "r"
    s.expiration_time = time.time() + 3600  # an hour from now
    # Far-future expiration -> no refresh.
    s._refresh_token_if_needed()  # no I/O


def test_apiserver_refresh_skipped_when_already_expired():
    s = a.APIServer("host")
    s.refresh_token = "r"
    s.expiration_time = time.time() - 100  # already-expired
    # Already-expired triggers the `already_expired` short-circuit
    # (the legacy logic refuses to refresh past-deadline tokens).
    s._refresh_token_if_needed()


def test_apiserver_refresh_invoked_when_within_buffer(monkeypatch):
    s = a.APIServer("host")
    s.refresh_token = "r"
    # Inside the refresh buffer but not expired: refresh fires.
    s.expiration_time = time.time() + a.REFRESH_BUFFER_IN_SECONDS - 10

    called: dict = {}

    def fake_refresh():
        called["yes"] = True

    monkeypatch.setattr(s, "authentication_refresh", fake_refresh)
    s._refresh_token_if_needed()
    assert called == {"yes": True}


class Reply:
    def __init__(self, status_code):
        self.status_code = status_code


def session(tmp_path, bearer="b0", refresh="r0", expires_in=3600):
    s = a.APIServer("host", credentials_file=str(tmp_path / "connection.json"))
    s.bearer_token = bearer
    s.refresh_token = refresh
    s.token_type = "Bearer"
    s.expires_in = expires_in
    s.expiration_time = time.time() + expires_in
    s._store_in_file(s.credentials_file)
    return s


def another_process_refreshes(s, bearer, refresh):
    other = a.APIServer("host", credentials_file=s.credentials_file)
    other.adopt(s.credentials())
    other.bearer_token = bearer
    other.refresh_token = refresh
    other.expiration_time = time.time() + 3600
    other._store_in_file(other.credentials_file)


def test_a_request_rejected_after_another_process_refreshed_retries_with_its_session(
    tmp_path, monkeypatch
):
    s = session(tmp_path)
    another_process_refreshes(s, "b1", "r1")
    sent = []

    def request_once(method, path, content_type=None, **kwargs):
        sent.append(s.bearer_token)
        return Reply(401 if s.bearer_token == "b0" else 200)

    monkeypatch.setattr(s, "request_once", request_once)
    assert s.get("/x").status_code == 200
    assert sent == ["b0", "b1"]
    assert s.refresh_token == "r1"


def test_a_rejected_request_waits_for_a_refresh_still_in_flight(tmp_path, monkeypatch):
    s = session(tmp_path)
    monkeypatch.setattr(a, "AUTH_RETRY_FIRST_PAUSE", 0.01)
    attempts = []

    def request_once(method, path, content_type=None, **kwargs):
        attempts.append(s.bearer_token)
        return Reply(401 if s.bearer_token == "b0" else 200)

    real_sleep = time.sleep
    pauses = []

    def sleep(seconds):
        pauses.append(seconds)
        if len(pauses) == 3:
            another_process_refreshes(s, "b1", "r1")
        real_sleep(0)

    monkeypatch.setattr(s, "request_once", request_once)
    monkeypatch.setattr(a.time, "sleep", sleep)
    assert s.post("/x", json={}).status_code == 200
    assert attempts == ["b0", "b1"]
    assert pauses == [0.01, 0.02, 0.04]


def test_a_rejected_request_gives_up_after_the_retry_window(tmp_path, monkeypatch):
    s = session(tmp_path)
    monkeypatch.setattr(a, "AUTH_RETRY_SECONDS", 0.2)
    monkeypatch.setattr(a, "AUTH_RETRY_FIRST_PAUSE", 0.01)
    monkeypatch.setattr(s, "request_once", lambda *args, **kwargs: Reply(401))
    started = time.monotonic()
    assert s.delete("/x").status_code == 401
    assert 0.2 <= time.monotonic() - started < 2


def test_an_expired_session_fails_without_waiting(tmp_path, monkeypatch):
    s = session(tmp_path, expires_in=-10)
    calls = []

    def request_once(*args, **kwargs):
        calls.append(1)
        return Reply(401)

    monkeypatch.setattr(s, "request_once", request_once)
    started = time.monotonic()
    assert s.get("/x").status_code == 401
    assert time.monotonic() - started < 1
    assert calls == [1]


def test_an_expired_process_adopts_the_session_another_process_refreshed(
    tmp_path, monkeypatch
):
    s = session(tmp_path, expires_in=-10)
    another_process_refreshes(s, "b1", "r1")
    sent = []

    def request_once(method, path, content_type=None, **kwargs):
        sent.append(s.bearer_token)
        return Reply(200)

    monkeypatch.setattr(s, "request_once", request_once)
    assert s.get("/x").status_code == 200
    assert sent == ["b1"]


def test_a_service_token_or_login_request_is_never_retried(tmp_path, monkeypatch):
    s = session(tmp_path)
    s.refresh_token = None
    calls = []

    def request_once(*args, **kwargs):
        calls.append(1)
        return Reply(401)

    monkeypatch.setattr(s, "request_once", request_once)
    assert s.get("/x").status_code == 401
    s.refresh_token = "r0"
    assert s.post("/auth/login", json={}, refresh_if_needed=False).status_code == 401
    assert calls == [1, 1]


def test_a_refresh_adopts_the_session_another_process_already_refreshed(
    tmp_path, monkeypatch
):
    s = session(tmp_path, expires_in=a.REFRESH_BUFFER_IN_SECONDS - 10)
    another_process_refreshes(s, "b1", "r1")
    monkeypatch.setattr(
        s, "authentication_refresh", lambda: pytest.fail("refreshed twice")
    )
    s._refresh_token_if_needed()
    assert (s.bearer_token, s.refresh_token) == ("b1", "r1")


def test_a_refresh_happens_once_under_the_lock_and_is_stored(tmp_path, monkeypatch):
    s = session(tmp_path, expires_in=a.REFRESH_BUFFER_IN_SECONDS - 10)

    def refresh():
        s.bearer_token, s.refresh_token = "b1", "r1"
        s.expiration_time = time.time() + 3600
        s._store_in_file(s.credentials_file)

    monkeypatch.setattr(s, "authentication_refresh", refresh)
    s._refresh_token_if_needed()
    late = a.APIServer("host", credentials_file=s.credentials_file)
    late.adopt(s.credentials())
    late.bearer_token, late.refresh_token = "b0", "r0"
    late.expiration_time = time.time() + a.REFRESH_BUFFER_IN_SECONDS - 10
    monkeypatch.setattr(
        late, "authentication_refresh", lambda: pytest.fail("refreshed twice")
    )
    late._refresh_token_if_needed()
    assert (late.bearer_token, late.refresh_token) == ("b1", "r1")


def test_a_failed_refresh_lets_the_request_go_on(tmp_path, monkeypatch):
    s = session(tmp_path, expires_in=a.REFRESH_BUFFER_IN_SECONDS - 10)

    def refresh():
        raise a.APIServerError({"error_description": "Invalid refresh token"})

    monkeypatch.setattr(s, "authentication_refresh", refresh)
    monkeypatch.setattr(s, "request_once", lambda *args, **kwargs: Reply(200))
    assert s.get("/x").status_code == 200


def test_a_credentials_file_of_another_server_is_ignored(tmp_path):
    s = session(tmp_path)
    other = a.APIServer("elsewhere", credentials_file=s.credentials_file)
    assert other.stored_credentials() is None


# ---------------------------------------------------------------------------
# Project scoping: `_scoped` builds `/projects/{project}/...` paths from the
# active project; scoping is a per-endpoint decision, so paths not built
# through it go out verbatim. `_project_narrow` narrows cross-project lists.
# ---------------------------------------------------------------------------


def test_apiserver_starts_with_no_project():
    assert a.APIServer("host").project is None


def test_scoped_requires_a_project():
    s = a.APIServer("host")
    with pytest.raises(a.NoProjectSelectedError):
        s._scoped("/collections")


def test_scoped_prefixes_the_active_project():
    s = a.APIServer("host")
    s.project = "acme"
    assert s._scoped("/collections") == "/projects/acme/collections"
    assert (
        s._scoped("/collections/c1/functions")
        == "/projects/acme/collections/c1/functions"
    )


def test_project_narrow_appends_filter_when_project_set(monkeypatch):
    s = a.APIServer("host")
    assert s._project_narrow(None) is None
    assert s._project_narrow("status:eq:D") == "status:eq:D"
    s.project = "acme"
    monkeypatch.setattr(s, "_active_project_name", lambda: "acme")
    assert s._project_narrow(None) == ["project:eq:acme"]
    assert s._project_narrow("status:eq:D") == ["status:eq:D", "project:eq:acme"]
    assert s._project_narrow(["status:eq:D"]) == ["status:eq:D", "project:eq:acme"]


def test_spanning_suspends_and_restores_the_active_project():
    s = a.APIServer("host")
    s.project = "acme"
    with s.spanning():
        assert s.project is None
    assert s.project == "acme"


def test_spanning_restores_the_active_project_on_error():
    s = a.APIServer("host")
    s.project = "acme"
    with pytest.raises(RuntimeError):
        with s.spanning():
            raise RuntimeError("boom")
    assert s.project == "acme"


# ---------------------------------------------------------------------------
# Factory helpers + small static methods
# ---------------------------------------------------------------------------


def test_get_params_dict_filters_none_values():
    out = a.APIServer.get_params_dict(["a", "b", "c"], [1, None, "x"])
    assert out == {"a": 1, "c": "x"}


def test_get_params_dict_returns_empty_when_all_none():
    out = a.APIServer.get_params_dict(["a", "b"], [None, None])
    assert out == {}


def test_get_params_dict_includes_base_path_when_set():
    out = a.APIServer.get_params_dict(["name", "base_path"], ["c", "/data/x"])
    assert out == {"name": "c", "base_path": "/data/x"}


def test_get_params_dict_omits_base_path_when_none():
    out = a.APIServer.get_params_dict(["name", "base_path"], ["c", None])
    assert out == {"name": "c"}


def test_get_params_dict_keeps_false_reset_flags():
    # ``False`` is a meaningful reset value and must survive the filter
    # (only ``None`` is dropped).
    out = a.APIServer.get_params_dict(["reset_state", "reset_stage"], [False, True])
    assert out == {"reset_state": False, "reset_stage": True}


# ---------------------------------------------------------------------------
# Request-payload assembly for the base_path / reset_* params.
#
# These methods build the JSON body and hand it to ``self.post``; we
# stub ``post`` to capture the body without any network I/O.
# ---------------------------------------------------------------------------


class _CapturePost:
    """A ``post`` replacement that records its ``json=`` payload and
    returns a dummy 200 response so ``raise_for_status`` is a no-op."""

    def __init__(self):
        self.seen: dict = {}

    def __call__(self, path, data=None, json=None, params=None, **kw):
        self.seen["path"] = path
        self.seen["json"] = json
        return _DummyResponse()


class _DummyResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"data": {}}


def _server_with_captured_post(monkeypatch):
    server = a.APIServer("host")
    # collection/function endpoints are project-scoped; an active project must
    # ride on the connection for their paths to build.
    server.project = "proj"
    capture = _CapturePost()
    monkeypatch.setattr(server, "post", capture)
    return server, capture


def test_collection_create_includes_base_path_in_body(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.collection_create("coll", "desc", base_path="/data/coll")
    assert capture.seen["json"]["base_path"] == "/data/coll"


def test_collection_create_omits_base_path_when_none(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.collection_create("coll", "desc")
    assert "base_path" not in capture.seen["json"]


def test_collection_update_includes_base_path_in_body(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.collection_update("coll", base_path="/data/new")
    assert capture.seen["json"]["base_path"] == "/data/new"


def test_collection_update_omits_unset_fields(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.collection_update("coll", description="d")
    body = capture.seen["json"]
    assert body == {"description": "d"}
    assert "base_path" not in body


def test_function_register_includes_base_path_in_body(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.function_register(
        collection_name="coll",
        function_name="fn",
        description="d",
        tables=["t"],
        table_types={},
        dependencies=[],
        trigger_by=None,
        function_snippet="<src>",
        bundle_id="b1",
        decorator="P",
        runtime_values="{}",
        reuse_frozen_tables=False,
        base_path="/data/fn",
    )
    assert capture.seen["json"]["base_path"] == "/data/fn"


def test_function_update_forwards_base_path_and_reset_flags(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.function_update(
        collection_name="coll",
        function_name="fn",
        base_path="/data/fn",
        reset_state=True,
        reset_stage=True,
    )
    body = capture.seen["json"]
    assert body["base_path"] == "/data/fn"
    assert body["reset_state"] is True
    assert body["reset_stage"] is True


def test_function_update_omits_reset_flags_when_none(monkeypatch):
    server, capture = _server_with_captured_post(monkeypatch)
    server.function_update(collection_name="coll", function_name="fn")
    body = capture.seen["json"]
    assert "reset_state" not in body
    assert "reset_stage" not in body
    assert "base_path" not in body


class _CaptureGet:
    """A ``get`` replacement that records its path and returns a dummy 200
    response so ``raise_for_status`` is a no-op."""

    def __init__(self):
        self.seen: dict = {}

    def __call__(self, path, params=None, **kw):
        self.seen["path"] = path
        self.seen["params"] = params
        return _DummyResponse()


def test_function_bundle_download_targets_function_version_endpoint(monkeypatch):
    server = a.APIServer("host")
    capture = _CaptureGet()
    monkeypatch.setattr(server, "get", capture)
    server.function_bundle_download("fv-123")
    assert capture.seen["path"] == "/function-versions/fv-123/bundle"


def test_obtain_connection_returns_apiserver_without_login():
    conn = a.obtain_connection("host")
    assert isinstance(conn, a.APIServer)
    assert conn.bearer_token is None


def test_obtain_connection_triggers_login_when_creds_present(monkeypatch):
    seen: dict = {}

    def fake_login(self, name, password):
        seen["name"] = name
        self.bearer_token = "tok"

    monkeypatch.setattr(a.APIServer, "authentication_login", fake_login)
    conn = a.obtain_connection("host", name="alice", password="pw")
    assert conn.bearer_token == "tok"
    assert seen == {"name": "alice"}


# ---------------------------------------------------------------------------
# configure_request_connection_pool
# ---------------------------------------------------------------------------


def test_configure_request_connection_pool_returns_two_sessions():
    sessions = a.configure_request_connection_pool()
    assert len(sessions) == 2
    for session in sessions:
        assert "http://" in session.adapters
        assert "https://" in session.adapters


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------


def test_module_constants_are_well_defined():
    assert a.DEFAULT_APISERVER_PORT == "2457"
    assert a.HTTP_PROTOCOL == "http://"
    assert a.HTTPS_PROTOCOL == "https://"
    assert a.BASE_API_URL == "/api/v1"
    assert "len" in a.DEFAULT_LIST_ENDPOINT_PARAMETERS


# ---------------------------------------------------------------------------
# Transport outside pytest
# ---------------------------------------------------------------------------


def test_a_get_outside_pytest_survives_a_transient_server_error(monkeypatch):
    """Outside pytest (no PYTEST_CURRENT_TEST, as tdk runs), a GET
    that first hits a 503 from a restarting apiserver and would succeed on a
    retry must succeed, as it does under pytest."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    statuses = [503, 200]

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            status = statuses.pop(0) if len(statuses) > 1 else statuses[0]
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        api = a.APIServer(f"http://127.0.0.1:{server.server_address[1]}")
        response = api.get("/status", refresh_if_needed=False)
    finally:
        server.shutdown()
        server.server_close()

    assert response.status_code == 200


def write_against(method: str, statuses: list[int]) -> tuple[int, int]:
    received = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def answer(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            received.append(self.path)
            status = statuses.pop(0) if len(statuses) > 1 else statuses[0]
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_POST = answer
        do_DELETE = answer

        def log_message(self, *_a, **_kw):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        api = a.APIServer(f"http://127.0.0.1:{server.server_address[1]}")
        response = getattr(api, method)("/write", json={}, refresh_if_needed=False)
    finally:
        server.shutdown()
        server.server_close()
    return response.status_code, len(received)


@pytest.mark.parametrize("method", ["post", "delete"])
def test_a_write_survives_a_busy_server(monkeypatch, method):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    assert write_against(method, [503, 503, 200]) == (200, 3)


@pytest.mark.parametrize("method", ["post", "delete"])
def test_a_write_is_not_repeated_after_another_server_error(monkeypatch, method):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    assert write_against(method, [500, 200]) == (500, 1)
