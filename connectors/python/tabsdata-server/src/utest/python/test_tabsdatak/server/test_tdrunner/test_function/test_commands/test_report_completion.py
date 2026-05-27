#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import http.server
import socket
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.report_completion import (
    EXIT_BAD_MANIFEST,
    EXIT_BAD_REQUEST,
    EXIT_HTTP_ERROR,
    EXIT_MANIFEST_MISMATCH,
    EXIT_WRITE_FAILED,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers ---


def _set_argv(
    monkeypatch,
    *,
    url,
    token,
    request,
    outputs_manifest,
    state_dir,
    out,
    liveness,
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report-completion",
            "--url",
            str(url),
            "--token",
            str(token),
            "--request",
            str(request),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--out",
            str(out),
            "--liveness",
            str(liveness),
        ],
    )


def _write_request(path, *, output=None, system_output=None):
    body = {
        "info": {
            "collection": "c",
            "collection_id": 1,
            "execution_id": 2,
            "transaction_id": 3,
            "function_run_id": 4,
            "function_bundle": {"uri": "file:///bundle.tgz"},
        },
        "output": output or [],
        "system_output": system_output or [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


def _write_manifest(path: Path, entries: list) -> Path:
    path.write_text(yaml.safe_dump({"outputs": entries}))
    return path


def _write_parquet(path: Path, **columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(columns)
    pq.write_table(table, path)


# --- HTTP fixture ---


@pytest.fixture
def http_server():
    """Tiny HTTP server in a thread. Yields (base_url, request_log, server)."""
    requests_log = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a, **kw):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            requests_log.append({
                "path": self.path,
                "headers": dict(self.headers),
                "body": body,
            })
            status = getattr(self.server, "next_status", 200)
            self.send_response(status)
            self.end_headers()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.next_status = 200
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", requests_log, server
    finally:
        server.shutdown()
        server.server_close()


# --- happy path ---


def test_main_writes_response_and_posts(monkeypatch, tmp_path, http_server):
    base_url, request_log, _ = http_server
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[{"name": "customers"}],
        system_output=[{"name": "td-initial-values"}],
    )
    state_dir = tmp_path / "state"
    customers_parquet = tmp_path / "out" / "slot0" / "customers.parquet"
    _write_parquet(customers_parquet, id=[1, 2, 3], name=["a", "b", "c"])
    _write_parquet(state_dir / "td-initial-values.parquet", offset=[42])
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "customers", "file": str(customers_parquet)}],
    )

    out = tmp_path / "response.yaml"
    _set_argv(
        monkeypatch,
        url=f"{base_url}/api/v1/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=state_dir,
        out=out,
        liveness=tmp_path / "live",
    )
    assert main() == 0

    body = out.read_bytes()
    parsed = yaml.safe_load(body)
    by_name = {e["table"]: e for e in parsed["output"]}
    assert by_name["customers"]["info"]["row_count"] == 3
    assert by_name["customers"]["info"]["column_count"] == 2
    assert by_name["customers"]["info"]["schema_hash"].startswith("sha256:")
    assert by_name["td-initial-values"]["info"]["row_count"] == 1
    assert by_name["td-initial-values"]["info"]["column_count"] == 1

    # POST happened with correct headers and body
    assert len(request_log) == 1
    posted = request_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer tok"
    assert posted["body"] == body


def test_main_no_info_when_manifest_file_is_null(monkeypatch, tmp_path, http_server):
    """A manifest entry with `file: null` (user returned None for that
    slot) yields a NoData entry in the response."""
    base_url, _, _ = http_server
    request_yaml = _write_request(
        tmp_path / "request.yaml", output=[{"name": "missing"}]
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "missing", "file": None}],
    )
    out = tmp_path / "response.yaml"
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "missing"}


def test_main_no_info_when_state_parquet_missing(monkeypatch, tmp_path, http_server):
    """A request.system_output table whose parquet doesn't exist in
    state_dir yields a NoData entry."""
    base_url, _, _ = http_server
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_output=[{"name": "unwritten-state"}],
    )
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "response.yaml"
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "unwritten-state"}


def test_main_resolves_env_refs(monkeypatch, tmp_path, http_server):
    base_url, request_log, _ = http_server
    monkeypatch.setenv("TD_TEST_RC_URL", f"{base_url}/done")
    monkeypatch.setenv("TD_TEST_RC_TOK", "secret")
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url="env:TD_TEST_RC_URL",
        token="env:TD_TEST_RC_TOK",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == 0
    assert request_log[0]["headers"]["Authorization"] == "Bearer secret"


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_server):
    base_url, _, _ = http_server
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=tmp_path / "no-request.yaml",
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_BAD_REQUEST


def test_main_missing_manifest_returns_bad_manifest(monkeypatch, tmp_path, http_server):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=tmp_path / "no-manifest.yaml",
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_BAD_MANIFEST


def test_main_malformed_manifest_returns_bad_manifest(
    monkeypatch, tmp_path, http_server
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad = tmp_path / "outputs_manifest.yaml"
    bad.write_text("outputs: [{unclosed")
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=bad,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_BAD_MANIFEST


def test_main_output_table_missing_from_manifest_returns_mismatch(
    monkeypatch, tmp_path, http_server
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml", output=[{"name": "ghost"}])
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_MANIFEST_MISMATCH


def test_main_http_error_returns_http_error_but_writes_out(
    monkeypatch, tmp_path, http_server
):
    base_url, _, server = http_server
    server.next_status = 500
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "response.yaml"
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_HTTP_ERROR
    # --out is written before POST so it survives the failure.
    assert out.exists()


def test_main_url_error_returns_http_error(monkeypatch, tmp_path):
    """URLError (network unreachable) is mapped to EXIT_HTTP_ERROR."""

    def fake(req):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url="http://127.0.0.1:1/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_HTTP_ERROR


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path, http_server):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        token="tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "no" / "such" / "response.yaml",
        liveness=tmp_path / "live",
    )
    assert main() == EXIT_WRITE_FAILED


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["report-completion"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_report_completion"
