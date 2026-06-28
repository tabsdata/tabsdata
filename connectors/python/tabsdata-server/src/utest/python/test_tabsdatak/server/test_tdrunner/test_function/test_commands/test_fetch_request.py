#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.fetch_request import _fetch, main
from tabsdatak.server._tdrunner.function.commands.fetch_request.error import (
    BadPayloadError,
    HttpFetchError,
    WriteFailedError,
)


def _envelope_bytes(data) -> bytes:
    return json.dumps({
        "version": "1",
        "context": {"errors": [], "warnings": [], "notifications": []},
        "data": data,
    }).encode("utf-8")


pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- _fetch (unit) ---


class _Resp:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_fetch_returns_body_and_passes_authorization_verbatim(monkeypatch):
    captured: dict = {}

    def fake(req):
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["headers"] = dict(req.headers)
        return _Resp(b"yaml-body")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    body = _fetch("https://api/r", "GET", "Bearer raw-token")
    assert body == b"yaml-body"
    assert captured["url"] == "https://api/r"
    assert captured["method"] == "GET"
    # urllib title-cases header keys.
    assert captured["headers"]["Authorization"] == "Bearer raw-token"


def test_fetch_http_error_raises_typed_exception(monkeypatch):
    def fake(req):
        raise urllib.error.HTTPError("https://api/r", 404, "Not Found", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(HttpFetchError) as exc_info:
        _fetch("https://api/r", "GET", "Bearer tok")
    assert exc_info.value.exit_code == HttpFetchError.exit_code
    assert "404" in str(exc_info.value)


def test_fetch_http_error_includes_response_body_in_detail(monkeypatch):
    """HTTPError's response body (apiserver's specific complaint) must
    end up in the raised HttpFetchError's detail — otherwise a 4xx like
    'token scope mismatch' bottoms out at a bare 'Not Found' and forces
    a manual chase to figure out what the apiserver objected to.
    """
    import io

    apiserver_body = b'{"code":"AuthError::0001","error":"scope_mismatch"}'

    def fake(req):
        raise urllib.error.HTTPError(
            "https://api/r",
            404,
            "Not Found",
            {},
            io.BytesIO(apiserver_body),
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(HttpFetchError) as exc_info:
        _fetch("https://api/r", "GET", "Bearer tok")
    rendered = str(exc_info.value)
    assert "Not Found" in rendered
    assert apiserver_body.decode("utf-8") in rendered


def test_fetch_url_error_raises_typed_exception(monkeypatch):
    def fake(req):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(HttpFetchError) as exc_info:
        _fetch("https://api/r", "GET", "Bearer tok")
    assert exc_info.value.exit_code == HttpFetchError.exit_code
    assert "connection refused" in str(exc_info.value)


# --- main (integration via monkeypatched urlopen) ---


def _set_argv(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["fetch-request", *args])


def _stub_urlopen(monkeypatch, body: bytes = b"data", capture: dict | None = None):
    def fake(req):
        if capture is not None:
            capture["url"] = req.full_url
            capture["method"] = req.get_method()
            capture["headers"] = dict(req.headers)
        return _Resp(body)

    monkeypatch.setattr(urllib.request, "urlopen", fake)


def _required_args(
    monkeypatch,
    *,
    request_file,
    liveness,
    resolver_url="https://api/r/1",
    resolver_method="GET",
    resolver_authorization="Bearer rtok",
    responder_url="https://api/w/1",
    responder_method="POST",
    responder_authorization="Bearer wtok",
    extra=(),
):
    _set_argv(
        monkeypatch,
        "--resolver-url",
        resolver_url,
        "--resolver-method",
        resolver_method,
        "--resolver-authorization",
        resolver_authorization,
        "--responder-url",
        responder_url,
        "--responder-method",
        responder_method,
        "--responder-authorization",
        responder_authorization,
        "--journal-file",
        "/tmp/em.yaml",
        "--request-file",
        str(request_file),
        "--liveness",
        str(liveness),
        *extra,
    )


def test_main_extracts_data_field_and_writes_yaml(monkeypatch, tmp_path):
    captured: dict = {}
    payload = {"Function": {"info": {"function": "publish_people"}}}
    _stub_urlopen(monkeypatch, body=_envelope_bytes(payload), capture=captured)
    request_file = tmp_path / "request.yaml"
    liveness = tmp_path / "live"
    _required_args(
        monkeypatch,
        request_file=request_file,
        liveness=liveness,
        resolver_url="https://api/r/1",
        resolver_authorization="Bearer rtok",
    )
    rc = main()
    assert rc == 0
    written = yaml.safe_load(request_file.read_text())
    assert written == payload
    assert captured["url"] == "https://api/r/1"
    assert captured["method"] == "GET"
    assert captured["headers"]["Authorization"] == "Bearer rtok"
    # Liveness file was touched at least once (the Liveness `start()` does
    # an immediate touch).
    assert liveness.exists()


def test_main_resolves_env_refs(monkeypatch, tmp_path):
    monkeypatch.setenv("TD_TEST_FR_URL", "https://api/r/9")
    monkeypatch.setenv("TD_TEST_FR_METHOD", "GET")
    monkeypatch.setenv("TD_TEST_FR_AUTH", "Bearer secrettok")
    captured: dict = {}
    _stub_urlopen(monkeypatch, body=_envelope_bytes({"x": 1}), capture=captured)
    request_file = tmp_path / "out"
    _required_args(
        monkeypatch,
        request_file=request_file,
        liveness=tmp_path / "live",
        resolver_url="env:TD_TEST_FR_URL",
        resolver_method="env:TD_TEST_FR_METHOD",
        resolver_authorization="env:TD_TEST_FR_AUTH",
    )
    rc = main()
    assert rc == 0
    assert captured["url"] == "https://api/r/9"
    assert captured["method"] == "GET"
    assert captured["headers"]["Authorization"] == "Bearer secrettok"


def test_main_undefined_env_returns_2(monkeypatch, tmp_path):
    monkeypatch.delenv("TD_TEST_FR_NOPE", raising=False)
    _stub_urlopen(monkeypatch, body=_envelope_bytes({}))
    _required_args(
        monkeypatch,
        request_file=tmp_path / "out",
        liveness=tmp_path / "live",
        resolver_url="env:TD_TEST_FR_NOPE",
    )
    assert main() == 2


def test_main_http_error_returns_command_specific_exit(
    monkeypatch, tmp_path, http_ok_url
):
    real_urlopen = urllib.request.urlopen

    def fake(req, *args, **kwargs):
        # Let responder calls (POSTs to the local http_ok_url) pass
        # through to the real urlopen; fail only resolver GETs.
        if req.full_url.startswith(http_ok_url):
            return real_urlopen(req, *args, **kwargs)
        raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    _required_args(
        monkeypatch,
        request_file=tmp_path / "out",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == HttpFetchError.exit_code


def test_main_write_error_returns_command_specific_exit(monkeypatch, tmp_path):
    _stub_urlopen(monkeypatch, body=_envelope_bytes({}))
    # Parent path component is an existing file → mkdir(parents=True) fails
    # with NotADirectoryError (subclass of OSError) → WriteFailedError.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    _required_args(
        monkeypatch,
        request_file=blocker / "subdir" / "out",
        liveness=tmp_path / "live",
    )
    assert main() == WriteFailedError.exit_code


def test_main_creates_parent_folders_when_absent(monkeypatch, tmp_path):
    """The command auto-creates missing parents under --request-file."""
    payload = {"hello": "world"}
    _stub_urlopen(monkeypatch, body=_envelope_bytes(payload))
    nested = tmp_path / "deep" / "nested" / "path" / "request.yaml"
    _required_args(
        monkeypatch,
        request_file=nested,
        liveness=tmp_path / "live",
    )
    assert main() == 0
    assert yaml.safe_load(nested.read_text()) == payload


def test_main_malformed_json_returns_bad_payload(monkeypatch, tmp_path):
    _stub_urlopen(monkeypatch, body=b"not json at all")
    _required_args(
        monkeypatch,
        request_file=tmp_path / "out",
        liveness=tmp_path / "live",
    )
    assert main() == BadPayloadError.exit_code


def test_main_envelope_missing_data_field_returns_bad_payload(monkeypatch, tmp_path):
    _stub_urlopen(
        monkeypatch,
        body=json.dumps({"version": "1", "context": {}}).encode("utf-8"),
    )
    _required_args(
        monkeypatch,
        request_file=tmp_path / "out",
        liveness=tmp_path / "live",
    )
    assert main() == BadPayloadError.exit_code


def test_main_missing_required_option_returns_1(monkeypatch, tmp_path):
    _set_argv(
        monkeypatch,
        "--resolver-url",
        "https://api/r",
        # missing every other required arg.
    )
    assert main() == 1
