#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator

import obstore
import pytest

from tabsdatak.common._storage import mounts
from tabsdatak.common._storage.transfer import OBJECT_STORES
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
# ``_commons/document/message/request.py``), so this dict must contain only fields the
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
        "project": "p",
        "collection_id": 1,
        "collection": "c",
        "function_id": "F-1",
        "function": "fn",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function_bundle": {"uri": bundle_uri, "env_prefix": "ROOT"},
        "function_stage": {"uri": "file:///stage", "env_prefix": "ROOT"},
        "execution_id": 2,
        "transaction_id": 3,
        "logical_function_run_id": 4,
        "physical_function_run_id": 4,
        "triggered_on": 1780000000100,
        "function_data": {"uri": "file:///data", "env_prefix": "ROOT"},
        "function_landing": {"uri": "file:///data/r", "env_prefix": "ROOT"},
        "function_scratch": {"uri": "file:///data/s", "env_prefix": "ROOT"},
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


def input_table(entry: dict, index: int) -> dict:
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "input_idx": index,
        "table_pos": index,
        "version_pos": 0,
        **entry,
    }
    return {"Table": table}


def output_table(entry: dict, index: int) -> dict:
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "logical_table_data_version_id": "LDV-1",
        "physical_table_data_version_id": "PDV-1",
        "location": {"uri": "file:///irrelevant", "env_prefix": "ROOT"},
        "table_pos": index,
        **entry,
    }
    return {"Table": table}


RUN_MOUNT = "ROOT"


def emulated_run(domain: Any, monkeypatch: pytest.MonkeyPatch) -> str:
    for key, value in domain.storage_options.items():
        monkeypatch.setenv(
            mounts.secret_variable(
                mounts.STORAGE_KEY,
                mounts.MOUNTS_KEY,
                RUN_MOUNT,
                mounts.OPTIONS_KEY,
                key,
            ),
            value,
        )
    return f"{domain.uri.rstrip('/')}/run-{uuid.uuid4().hex}"


def run_location(run: str, folder: str) -> dict[str, str]:
    return {"uri": f"{run}/{folder}", "env_prefix": RUN_MOUNT}


def stored(location: dict[str, str], domain: Any) -> list[str]:
    store = OBJECT_STORES.store(location["uri"], dict(domain.storage_options))
    return [item["path"] for batch in obstore.list(store) for item in batch]
