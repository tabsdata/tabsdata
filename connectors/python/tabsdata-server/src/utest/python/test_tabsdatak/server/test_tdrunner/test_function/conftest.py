#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator

import pytest

from tabsdatak.server._tdrunner.function._commons.http import retry

# --- Shared synthetic Request.info builder --------------------------------
#
# Single source of truth for the ``Function.info`` payload used by
# tests that need a request.yaml on disk. Each command's ``test_*.py``
# previously defined its own near-identical ``_info`` (or inlined the
# dict in ``_write_request``) -- eight copies in total, each missing
# every new required ``Info`` field at the moment the schema changed
# (TD-2471 added ``project_id`` and broke ~60 tests). Centralising here
# means adding a new required field is a one-line change instead of a
# fan-out across every test module.
#
# Note: ``Info`` is declared with ``extra="forbid"`` (see
# ``_lib/request.py``), so this dict must contain only fields the
# Pydantic model knows about. Defaults are filler values; override any
# subset by keyword for per-test specifics (e.g.
# ``execution_id="exec-42"``).


def info_payload(
    bundle_uri: str = "file:///bundle.tar.zst",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a synthetic ``Function.info`` dict.

    ``bundle_uri`` is the most-commonly-overridden field, so it's a
    positional shortcut; everything else goes through ``**overrides``.
    """
    base: dict[str, Any] = {
        "project_id": "P-1",
        "collection_id": 1,
        "collection": "c",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function": "fn",
        "function_run_id": 4,
        "function_bundle": {"uri": bundle_uri, "env_prefix": "ROOT"},
        "function_stage": {"uri": "file:///stage", "env_prefix": "ROOT"},
        "triggered_on": 1780000000100,
        "transaction_id": 3,
        "execution_id": 2,
        "function_data": {"uri": "file:///data", "env_prefix": "ROOT"},
        "scheduled_on": 1780000000200,
    }
    base.update(overrides)
    return base


class _OkHandler(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._ok()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        self._ok()

    def log_message(self, format, *args):
        pass


@pytest.fixture
def http_ok_url() -> Iterator[str]:
    """An always-200 local HTTP server. Yields its base URL.

    Used by error-path tests that would otherwise stall for the whole
    framework's responder retry budget when the responder URL can't be
    reached. Pointing `--responder-url` here lets the real POST succeed
    immediately while exercising the genuine code path.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OkHandler)
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def no_http_retry(monkeypatch):
    monkeypatch.setattr(retry, "TOTAL_SECONDS", 0)
    monkeypatch.setattr(retry, "WAIT_SECONDS", 0)
