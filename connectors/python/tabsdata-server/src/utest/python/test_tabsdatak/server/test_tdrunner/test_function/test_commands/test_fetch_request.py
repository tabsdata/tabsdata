#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
import urllib.error
import urllib.request

import pytest

from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function.commands.fetch_request import (
    EXIT_HTTP_ERROR,
    EXIT_WRITE_ERROR,
    _fetch,
    main,
)

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


def test_fetch_returns_body_and_sets_bearer_auth(monkeypatch):
    captured: dict = {}

    def fake(req):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.headers)
        return _Resp(b"yaml-body")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    body = _fetch("https://api/r", "tok")
    assert body == b"yaml-body"
    assert captured["url"] == "https://api/r"
    # urllib title-cases header keys.
    assert captured["headers"]["Authorization"] == "Bearer tok"


def test_fetch_http_error_raises_command_specific(monkeypatch):
    def fake(req):
        raise urllib.error.HTTPError("https://api/r", 404, "Not Found", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(CommandSpecificError) as exc_info:
        _fetch("https://api/r", "tok")
    assert exc_info.value.exit_code == EXIT_HTTP_ERROR
    assert "404" in str(exc_info.value)


def test_fetch_url_error_raises_command_specific(monkeypatch):
    def fake(req):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(CommandSpecificError) as exc_info:
        _fetch("https://api/r", "tok")
    assert exc_info.value.exit_code == EXIT_HTTP_ERROR
    assert "connection refused" in str(exc_info.value)


# --- main (integration via monkeypatched urlopen) ---


def _set_argv(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["fetch-request", *args])


def _stub_urlopen(monkeypatch, body: bytes = b"data", capture: dict | None = None):
    def fake(req):
        if capture is not None:
            capture["url"] = req.full_url
            capture["headers"] = dict(req.headers)
        return _Resp(body)

    monkeypatch.setattr(urllib.request, "urlopen", fake)


def test_main_writes_body_to_out_with_literal_args(monkeypatch, tmp_path):
    captured: dict = {}
    _stub_urlopen(monkeypatch, body=b"YAML CONTENT", capture=captured)
    out = tmp_path / "request.yaml"
    liveness = tmp_path / "live"
    _set_argv(
        monkeypatch,
        "--url",
        "https://api/r/1",
        "--token",
        "tok",
        "--out",
        str(out),
        "--liveness",
        str(liveness),
    )
    rc = main()
    assert rc == 0
    assert out.read_bytes() == b"YAML CONTENT"
    assert captured["url"] == "https://api/r/1"
    assert captured["headers"]["Authorization"] == "Bearer tok"
    # Liveness file was touched at least once (the Liveness `start()` does
    # an immediate touch).
    assert liveness.exists()


def test_main_resolves_env_refs(monkeypatch, tmp_path):
    monkeypatch.setenv("TD_TEST_FR_URL", "https://api/r/9")
    monkeypatch.setenv("TD_TEST_FR_TOK", "secrettok")
    captured: dict = {}
    _stub_urlopen(monkeypatch, body=b"x", capture=captured)
    out = tmp_path / "out"
    _set_argv(
        monkeypatch,
        "--url",
        "env:TD_TEST_FR_URL",
        "--token",
        "env:TD_TEST_FR_TOK",
        "--out",
        str(out),
        "--liveness",
        str(tmp_path / "live"),
    )
    rc = main()
    assert rc == 0
    assert captured["url"] == "https://api/r/9"
    assert captured["headers"]["Authorization"] == "Bearer secrettok"


def test_main_undefined_env_returns_2(monkeypatch, tmp_path):
    monkeypatch.delenv("TD_TEST_FR_NOPE", raising=False)
    _stub_urlopen(monkeypatch, body=b"x")
    _set_argv(
        monkeypatch,
        "--url",
        "env:TD_TEST_FR_NOPE",
        "--token",
        "tok",
        "--out",
        str(tmp_path / "out"),
        "--liveness",
        str(tmp_path / "live"),
    )
    assert main() == 2


def test_main_http_error_returns_command_specific_exit(monkeypatch, tmp_path):
    def fake(req):
        raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    _set_argv(
        monkeypatch,
        "--url",
        "https://api/r",
        "--token",
        "tok",
        "--out",
        str(tmp_path / "out"),
        "--liveness",
        str(tmp_path / "live"),
    )
    assert main() == EXIT_HTTP_ERROR


def test_main_write_error_returns_command_specific_exit(monkeypatch, tmp_path):
    _stub_urlopen(monkeypatch, body=b"x")
    _set_argv(
        monkeypatch,
        "--url",
        "https://api/r",
        "--token",
        "tok",
        # Parent dir doesn't exist → OSError on write_bytes.
        "--out",
        str(tmp_path / "no" / "such" / "dir" / "out"),
        "--liveness",
        str(tmp_path / "live"),
    )
    assert main() == EXIT_WRITE_ERROR


def test_main_missing_required_option_returns_1(monkeypatch, tmp_path):
    _set_argv(
        monkeypatch,
        "--url",
        "https://api/r",
        # missing --token, --out, --liveness
    )
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_fetch_request"
