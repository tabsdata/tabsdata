#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the low-level apiserver HTTP wrapper.

Network-touching code paths (`APIServer.get` / `post` / etc.) are
exercised end-to-end in the legacy integration test suite. Here we
test the pure-Python helpers: URL processing, error wrapping, the
auth-header property, the refresh-trigger gate, and the small
factory helpers."""

from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Project (tenant) scoping: `_resolve_path` injects the active project into
# tenant endpoints; globals pass through; a tenant call with no project fails
# fast rather than guessing a default.
# ---------------------------------------------------------------------------


def test_apiserver_starts_with_no_project():
    assert a.APIServer("host").project is None


@pytest.mark.parametrize(
    "path",
    ["/auth", "/projects", "/projects/acme", "/roles", "/status", "/users"],
)
def test_global_paths_pass_through_without_project(path):
    s = a.APIServer("host")
    assert s.project is None
    assert s._resolve_path(path) == path


def test_tenant_path_requires_a_project():
    s = a.APIServer("host")
    with pytest.raises(a.NoProjectSelectedError):
        s._resolve_path("/collections")


def test_tenant_path_is_prefixed_when_project_set():
    s = a.APIServer("host")
    s.project = "acme"
    assert s._resolve_path("/collections") == "/projects/acme/collections"
    assert (
        s._resolve_path("/collections/c1/functions")
        == "/projects/acme/collections/c1/functions"
    )
    # globals stay global even with a project set
    assert s._resolve_path("/projects") == "/projects"


# ---------------------------------------------------------------------------
# Factory helpers + small static methods
# ---------------------------------------------------------------------------


def test_get_params_dict_filters_none_values():
    out = a.APIServer.get_params_dict(["a", "b", "c"], [1, None, "x"])
    assert out == {"a": 1, "c": "x"}


def test_get_params_dict_returns_empty_when_all_none():
    out = a.APIServer.get_params_dict(["a", "b"], [None, None])
    assert out == {}


def test_obtain_connection_returns_apiserver_without_login():
    conn = a.obtain_connection("host")
    assert isinstance(conn, a.APIServer)
    assert conn.bearer_token is None


def test_obtain_connection_triggers_login_when_creds_present(monkeypatch):
    seen: dict = {}

    def fake_login(self, name, password, role=None):
        seen["name"] = name
        seen["role"] = role
        self.bearer_token = "tok"

    monkeypatch.setattr(a.APIServer, "authentication_login", fake_login)
    conn = a.obtain_connection("host", name="alice", password="pw", role="admin")
    assert conn.bearer_token == "tok"
    assert seen == {"name": "alice", "role": "admin"}


# ---------------------------------------------------------------------------
# configure_request_connection_pool
# ---------------------------------------------------------------------------


def test_configure_request_connection_pool_returns_two_sessions():
    default_session, get_session = a.configure_request_connection_pool()
    # Both have http+https adapters mounted.
    assert "http://" in default_session.adapters
    assert "https://" in default_session.adapters
    assert "http://" in get_session.adapters
    assert "https://" in get_session.adapters


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------


def test_module_constants_are_well_defined():
    assert a.DEFAULT_APISERVER_PORT == "2457"
    assert a.HTTP_PROTOCOL == "http://"
    assert a.HTTPS_PROTOCOL == "https://"
    assert a.BASE_API_URL == "/api/v1"
    assert a.PYTEST_CONTEXT_ACTIVE == "PYTEST_CURRENT_TEST"
    assert "len" in a.DEFAULT_LIST_ENDPOINT_PARAMETERS
