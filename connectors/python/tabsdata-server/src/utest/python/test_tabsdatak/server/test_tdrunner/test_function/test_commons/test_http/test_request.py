#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for `_commons/http/request.py`.

Covers:
  * `HttpRequest` / `HttpResponse` dataclass shape
  * `invoke` happy path (2xx)
  * `invoke` 4xx / 5xx are returned, not raised
  * `invoke` connection failure raises `HttpRequestError`
  * `invoke` query overlay merging
  * `apply_query` edge cases
"""

from __future__ import annotations

import http.server
import socket
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager

import pytest

from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpRequestError,
    HttpResponse,
    apply_query,
    invoke,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers --------------------------------------------------------------


@contextmanager
def _server(handler_cls):
    """Stand up a tiny HTTP server on a random port for the duration of
    a single test. Yields ``base_url``."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


def _silent_handler(status: int, body: bytes = b"", extra_headers=None):
    """Build a quiet-logging handler that always returns ``status``."""

    class _H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self._reply()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            self._reply()

        def _reply(self):
            self.send_response(status)
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, *_a, **_kw):
            pass

    return _H


# --- dataclass shape ------------------------------------------------------


def test_http_request_defaults_to_post_with_empty_headers():
    """The sidecar's common case -- caller only specifies the URL,
    POST + empty-headers come for free."""
    req = HttpRequest(url="http://example/")
    assert req.method == "POST"
    assert req.headers == {}


def test_http_request_is_frozen():
    """Frozen so the sidecar can safely reuse one descriptor across
    every manifest trigger -- no risk of accidental mutation."""
    req = HttpRequest(url="http://example/")
    with pytest.raises((AttributeError, TypeError)):
        req.url = "http://other/"  # type: ignore[misc]


def test_http_response_ok_property_true_for_2xx():
    assert HttpResponse(status=200, body=b"").ok is True
    assert HttpResponse(status=204, body=b"").ok is True
    assert HttpResponse(status=299, body=b"").ok is True


def test_http_response_ok_property_false_for_non_2xx():
    """3xx / 4xx / 5xx are not ``ok``. NU7 will key on this for
    triggered-vs-skip-vs-fail dispatch."""
    assert HttpResponse(status=199, body=b"").ok is False
    assert HttpResponse(status=300, body=b"").ok is False
    assert HttpResponse(status=409, body=b"").ok is False
    assert HttpResponse(status=500, body=b"").ok is False


# --- invoke: happy path ---------------------------------------------------


def test_invoke_returns_response_on_200():
    handler = _silent_handler(200, body=b"hello", extra_headers={"X-Foo": "bar"})
    with _server(handler) as base_url:
        resp = invoke(HttpRequest(url=base_url + "/path"))
    assert resp.status == 200
    assert resp.body == b"hello"
    assert resp.headers.get("x-foo") == "bar"  # lowercased
    assert resp.ok is True


def test_invoke_sends_body_when_provided():
    """Round-trips a POST body. NU7 typically sends fn_trigger args via
    query string and no body, but the helper supports both."""
    received: list[bytes] = []

    class _Echo(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            received.append(self.rfile.read(length))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    with _server(_Echo) as base_url:
        invoke(HttpRequest(url=base_url + "/"), body=b"hello-body")
    assert received == [b"hello-body"]


def test_invoke_sends_headers_when_provided():
    received: dict[str, str] = {}

    class _Capture(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            received.update({k.lower(): v for k, v in self.headers.items()})
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    with _server(_Capture) as base_url:
        invoke(
            HttpRequest(
                url=base_url + "/",
                headers={"Authorization": "Bearer secret", "X-Custom": "yes"},
            ),
        )
    assert received.get("authorization") == "Bearer secret"
    assert received.get("x-custom") == "yes"


# --- invoke: 4xx / 5xx don't raise ----------------------------------------


def test_invoke_returns_response_on_4xx_not_raises():
    """The whole point of this wrapper -- urllib's default behaviour
    raises HTTPError on non-2xx and hides the body from the caller."""
    handler = _silent_handler(409, body=b'{"error":"already running"}')
    with _server(handler) as base_url:
        resp = invoke(HttpRequest(url=base_url + "/"))
    assert resp.status == 409
    assert resp.body == b'{"error":"already running"}'
    assert resp.ok is False


def test_invoke_returns_response_on_5xx_not_raises():
    handler = _silent_handler(503, body=b"server unavailable")
    with _server(handler) as base_url:
        resp = invoke(HttpRequest(url=base_url + "/"))
    assert resp.status == 503
    assert resp.body == b"server unavailable"


def test_invoke_response_headers_lowercased_even_on_error():
    """Lowercased header keys are the consistent contract -- caller
    must be able to look up ``Content-Type`` the same way regardless
    of status."""
    handler = _silent_handler(404, extra_headers={"X-Apiserver-Reason": "no-such-fn"})
    with _server(handler) as base_url:
        resp = invoke(HttpRequest(url=base_url + "/"))
    assert resp.headers.get("x-apiserver-reason") == "no-such-fn"


# --- invoke: connection failures ------------------------------------------


def test_invoke_raises_http_request_error_on_connection_refused():
    """Bound-but-immediately-closed port -- nothing is listening, so
    urlopen surfaces URLError. Our wrapper translates it to
    HttpRequestError so callers see a single typed exception."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()  # release the port immediately

    with pytest.raises(HttpRequestError, match="failed"):
        invoke(
            HttpRequest(url=f"http://127.0.0.1:{port}/"),
            timeout=1.0,
        )


def test_invoke_raises_http_request_error_on_timeout(monkeypatch):
    """Patch urlopen to raise TimeoutError -- we surface as
    HttpRequestError so the sidecar can log + retry on its next poll
    instead of crashing on socket.timeout."""

    def _slow(*_args, **_kwargs):
        raise TimeoutError("simulated socket timeout")

    monkeypatch.setattr(urllib.request, "urlopen", _slow)
    with pytest.raises(HttpRequestError, match="timed out"):
        invoke(HttpRequest(url="http://127.0.0.1:1/"), timeout=0.1)


# --- apply_query ---------------------------------------------------------


def testapply_query_none_returns_url_unchanged():
    assert apply_query("http://x/p", None) == "http://x/p"


def testapply_query_empty_returns_url_unchanged():
    assert apply_query("http://x/p", {}) == "http://x/p"


def testapply_query_appends_to_url_without_query():
    assert (
        apply_query("http://x/p", {"skip_if_running": "true"})
        == "http://x/p?skip_if_running=true"
    )


def testapply_query_merges_with_existing_query():
    """Existing keys preserved, new keys added. URL parser will
    quote correctly."""
    out = apply_query("http://x/p?keep=1", {"new": "2"})
    # urlencode order may vary on impl; verify both pieces present
    assert "keep=1" in out
    assert "new=2" in out


def testapply_query_caller_keys_override_existing():
    """Caller-supplied wins -- the sidecar can pass a freshly-built
    ``external_id`` and override any stale value baked into the
    apiserver's base URL."""
    out = apply_query("http://x/p?stale=old", {"stale": "new"})
    assert "stale=new" in out
    assert "stale=old" not in out


def testapply_query_stringifies_values():
    """Values are stringified -- callers may pass ints, bools."""
    out = apply_query("http://x/p", {"size": 10, "active": True})
    assert "size=10" in out
    assert "active=True" in out


def test_invoke_uses_overlay_query_string():
    """End-to-end check: query passed via invoke() actually makes it
    onto the URL the server sees."""
    received_paths: list[str] = []

    class _Capture(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            received_paths.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    with _server(_Capture) as base_url:
        invoke(
            HttpRequest(url=base_url + "/fn_trigger"),
            query={"skip_if_running": "true", "external_id": "2026-06-16T10-30-12.345"},
        )
    assert len(received_paths) == 1
    path = received_paths[0]
    assert path.startswith("/fn_trigger?")
    assert "skip_if_running=true" in path
    assert "external_id=2026-06-16T10-30-12.345" in path
