#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import http.server
import json
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

from tabsdatak.server._tdrunner.function.commands.report_completion import _post, main
from tabsdatak.server._tdrunner.function.commands.report_completion.error import (
    BadManifestError,
    BadRequestError,
    HttpPostError,
    ManifestMismatchError,
    WriteFailedError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers ---


def _set_argv(
    monkeypatch,
    *,
    url,
    authorization,
    request,
    outputs_manifest,
    state_dir,
    out,
    liveness,
    responder_url,
    responder_method="POST",
    responder_authorization="Bearer tok",
    execution_meta_file="/tmp/execution_meta.yaml",
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report-completion",
            "--url",
            str(url),
            "--authorization",
            str(authorization),
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
            "--responder-url",
            responder_url,
            "--responder-method",
            responder_method,
            "--responder-authorization",
            responder_authorization,
            "--execution-meta-file",
            str(execution_meta_file),
        ],
    )


def _info():
    return {
        "collection": "c",
        "collection_id": 1,
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function": "fn",
        "function_run_id": 4,
        "function_bundle": {"uri": "file:///bundle.tgz", "env_prefix": "ROOT"},
        "function_stage": {"uri": "file:///stage", "env_prefix": "ROOT"},
        "triggered_on": 1780000000100,
        "transaction_id": 3,
        "execution_id": 2,
        "function_data": {"uri": "file:///data", "env_prefix": "ROOT"},
        "scheduled_on": 1780000000200,
    }


def _output_table(entry, index):
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "table_data_version_id": "TDV-1",
        "location": {"uri": "file:///out.parquet", "env_prefix": "ROOT"},
        "table_pos": index,
        **entry,
    }
    return {"Table": table}


def _write_request(path, *, output=None, system_output=None):
    body = {
        "FunctionV2": {
            "info": _info(),
            "output": [
                _output_table(entry, index) for index, entry in enumerate(output or [])
            ],
            "system_output": [
                _output_table(entry, index)
                for index, entry in enumerate(system_output or [])
            ],
        }
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


def test_main_writes_response_and_posts(
    monkeypatch, tmp_path, http_server, http_ok_url
):
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
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=state_dir,
        out=out,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0

    # The on-disk artifact stays YAML for human readability/audit; the
    # wire body is JSON (the apiserver expects application/json).
    yaml_artifact = yaml.safe_load(out.read_bytes())
    by_name = {e["table"]: e for e in yaml_artifact["output"]}
    assert by_name["customers"]["info"]["row_count"] == 3
    assert by_name["customers"]["info"]["column_count"] == 2
    assert by_name["customers"]["info"]["schema_hash"].startswith("sha256:")
    assert by_name["customers"]["info"]["data_bytes"] > 0
    assert by_name["td-initial-values"]["info"]["row_count"] == 1
    assert by_name["td-initial-values"]["info"]["column_count"] == 1
    assert by_name["td-initial-values"]["info"]["data_bytes"] > 0

    # POST happened with JSON content-type and a body that decodes to
    # the same structure as the YAML artifact.
    assert len(request_log) == 1
    posted = request_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer tok"
    assert posted["headers"]["Content-Type"] == "application/json"
    posted_payload = json.loads(posted["body"])
    posted_by_name = {e["table"]: e for e in posted_payload["output"]}
    assert (
        posted_by_name["customers"]["info"]["row_count"]
        == by_name["customers"]["info"]["row_count"]
    )


def test_main_no_info_when_manifest_file_is_null(
    monkeypatch, tmp_path, http_server, http_ok_url
):
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
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "missing"}


def test_main_no_info_when_state_parquet_missing(
    monkeypatch, tmp_path, http_server, http_ok_url
):
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
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "unwritten-state"}


def test_main_resolves_env_refs(monkeypatch, tmp_path, http_server, http_ok_url):
    base_url, request_log, _ = http_server
    monkeypatch.setenv("TD_TEST_RC_URL", f"{base_url}/done")
    monkeypatch.setenv("TD_TEST_RC_TOK", "Bearer secret")
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url="env:TD_TEST_RC_URL",
        authorization="env:TD_TEST_RC_TOK",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0
    assert request_log[0]["headers"]["Authorization"] == "Bearer secret"


# --- error paths ---


def test_main_missing_request_returns_bad_request(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, _ = http_server
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=tmp_path / "no-request.yaml",
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_missing_manifest_returns_bad_manifest(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=tmp_path / "no-manifest.yaml",
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


def test_main_malformed_manifest_returns_bad_manifest(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad = tmp_path / "outputs_manifest.yaml"
    bad.write_text("outputs: [{unclosed")
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=bad,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


def test_main_output_table_missing_from_manifest_returns_mismatch(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml", output=[{"name": "ghost"}])
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == ManifestMismatchError.exit_code


def test_main_http_error_returns_http_error_but_writes_out(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, server = http_server
    server.next_status = 500
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "response.yaml"
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == HttpPostError.exit_code
    # --out is written before POST so it survives the failure.
    assert out.exists()


def test_main_url_error_returns_http_error(monkeypatch, tmp_path, http_ok_url):
    """URLError (network unreachable) is mapped to HttpPostError."""

    real_urlopen = urllib.request.urlopen

    def fake(req, *args, **kwargs):
        if req.full_url.startswith(http_ok_url):
            return real_urlopen(req, *args, **kwargs)
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    _set_argv(
        monkeypatch,
        url="http://127.0.0.1:1/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == HttpPostError.exit_code


def test_post_http_error_includes_response_body_in_detail(monkeypatch):
    """HTTPError's response body (apiserver's specific complaint) must
    end up in the raised HttpPostError's detail — otherwise a 400 like
    'missing field `status`' bottoms out at a bare 'Bad Request' and
    forces a manual chase to figure out what the apiserver objected to.
    """
    import io

    apiserver_body = b'{"error":"missing field `status` at line 1 column 9"}'

    def fake_urlopen(req, *args, **kwargs):
        raise urllib.error.HTTPError(
            req.full_url,
            400,
            "Bad Request",
            {},
            io.BytesIO(apiserver_body),
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(HttpPostError) as exc_info:
        _post("http://api/done", "Bearer t", b"{}")
    rendered = str(exc_info.value)
    assert "Bad Request" in rendered
    assert apiserver_body.decode("utf-8") in rendered


def test_main_write_error_returns_write_failed(
    monkeypatch, tmp_path, http_server, http_ok_url
):
    base_url, _, _ = http_server
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    # Make the write fail by pointing `--out` *under* a regular file:
    # `write_response` mkdirs missing parents (TD-2338), but mkdir into
    # a path that already exists as a file raises OSError.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir")
    _set_argv(
        monkeypatch,
        url=f"{base_url}/done",
        authorization="Bearer tok",
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=blocker / "nested" / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == WriteFailedError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["report-completion"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_report_completion"
