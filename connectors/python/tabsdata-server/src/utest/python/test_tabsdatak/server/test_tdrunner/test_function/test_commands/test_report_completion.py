#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import argparse
import http.server
import io
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

from tabsdatak.server._tdrunner.function._lib import command as command_lib
from tabsdatak.server._tdrunner.function._lib.command import UnexpectedError
from tabsdatak.server._tdrunner.function.commands.report_completion import main
from tabsdatak.server._tdrunner.function.commands.report_completion.error import (
    BadManifestError,
    BadRequestError,
    ManifestMismatchError,
    WriteFailedError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@pytest.fixture
def no_responder_retry(monkeypatch):
    """Collapse the responder retry budget so a failing POST gives up on
    the first attempt instead of retrying for the full 32-second window."""
    monkeypatch.setattr(command_lib, "_RESPONDER_RETRY_TOTAL_SECONDS", 0)
    monkeypatch.setattr(command_lib, "_RESPONDER_RETRY_WAIT_SECONDS", 0)


# --- helpers ---


def _write_journal(path: Path, *, start: int = 1780000000000) -> Path:
    """Write a minimal valid journal so report-completion can stamp
    ``end`` and ``notify_done`` can read ``start`` / ``end`` back."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"start": start, "end": None}))
    return path


def _set_argv(
    monkeypatch,
    *,
    request,
    outputs_manifest,
    state_dir,
    out,
    liveness,
    responder_url,
    journal_file,
    responder_method="POST",
    responder_authorization="Bearer tok",
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report-completion",
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
            "--journal-file",
            str(journal_file),
        ],
    )


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
        "Function": {
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


# --- responder fixture ---


@pytest.fixture
def responder():
    """Tiny HTTP server in a thread acting as the responder endpoint.

    Yields (base_url, request_log, server). ``report-completion`` POSTs
    its ``Done`` callback (carrying the response as the ``WorkerOutput``
    context) here via ``--responder-url``.
    """
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


def test_main_writes_response_and_posts_done(monkeypatch, tmp_path, responder):
    base_url, request_log, _ = responder
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
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=state_dir,
        out=out,
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/api/v1/done",
        journal_file=journal,
    )
    assert main() == 0

    # The on-disk artifact stays YAML for human readability/audit.
    yaml_artifact = yaml.safe_load(out.read_bytes())
    by_name = {e["table"]: e for e in yaml_artifact["output"]}
    assert by_name["customers"]["info"]["row_count"] == 3
    assert by_name["customers"]["info"]["column_count"] == 2
    assert by_name["customers"]["info"]["schema_hash"].startswith("sha256:")
    assert by_name["customers"]["info"]["data_bytes"] > 0
    assert by_name["td-initial-values"]["info"]["row_count"] == 1
    assert by_name["td-initial-values"]["info"]["column_count"] == 1
    assert by_name["td-initial-values"]["info"]["data_bytes"] > 0

    # The responder received exactly one Done POST carrying the response
    # as the WorkerOutput context (JSON, externally-tagged Data/NoData).
    assert len(request_log) == 1
    posted = request_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer tok"
    assert posted["headers"]["Content-Type"] == "application/json"
    payload = json.loads(posted["body"])
    assert payload["status"] == "Done"
    written = payload["context"]["Function"]["output"]
    posted_by_name = {next(iter(entry.values()))["table"]: entry for entry in written}
    assert "Data" in posted_by_name["customers"]
    assert (
        posted_by_name["customers"]["Data"]["info"]["row_count"]
        == by_name["customers"]["info"]["row_count"]
    )


def test_main_no_info_when_manifest_file_is_null(monkeypatch, tmp_path, responder):
    """A manifest entry with `file: null` (user returned None for that
    slot) yields a NoData entry in the response."""
    base_url, request_log, _ = responder
    request_yaml = _write_request(
        tmp_path / "request.yaml", output=[{"name": "missing"}]
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "missing", "file": None}],
    )
    out = tmp_path / "response.yaml"
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "missing"}
    # NoData on the wire too.
    payload = json.loads(request_log[0]["body"])
    written = payload["context"]["Function"]["output"]
    assert written[0] == {"NoData": {"table": "missing"}}


def test_main_no_info_when_state_parquet_missing(monkeypatch, tmp_path, responder):
    """A request.system_output table whose parquet doesn't exist in
    state_dir yields a NoData entry."""
    base_url, _, _ = responder
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_output=[{"name": "unwritten-state"}],
    )
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "response.yaml"
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == 0
    parsed = yaml.safe_load(out.read_text())
    assert parsed["output"][0] == {"table": "unwritten-state"}


def test_main_resolves_responder_env_refs(monkeypatch, tmp_path, responder):
    base_url, request_log, _ = responder
    monkeypatch.setenv("TD_TEST_RC_URL", f"{base_url}/done")
    monkeypatch.setenv("TD_TEST_RC_TOK", "Bearer secret")
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url="env:TD_TEST_RC_URL",
        responder_authorization="env:TD_TEST_RC_TOK",
        journal_file=journal,
    )
    assert main() == 0
    assert request_log[0]["headers"]["Authorization"] == "Bearer secret"


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, responder):
    base_url, _, _ = responder
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=tmp_path / "no-request.yaml",
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == BadRequestError.exit_code


def test_main_missing_manifest_returns_bad_manifest(monkeypatch, tmp_path, responder):
    base_url, _, _ = responder
    request_yaml = _write_request(tmp_path / "request.yaml")
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=tmp_path / "no-manifest.yaml",
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == BadManifestError.exit_code


def test_main_malformed_manifest_returns_bad_manifest(monkeypatch, tmp_path, responder):
    base_url, _, _ = responder
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad = tmp_path / "outputs_manifest.yaml"
    bad.write_text("outputs: [{unclosed")
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=bad,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == BadManifestError.exit_code


def test_main_output_table_missing_from_manifest_returns_mismatch(
    monkeypatch, tmp_path, responder
):
    base_url, _, _ = responder
    request_yaml = _write_request(tmp_path / "request.yaml", output=[{"name": "ghost"}])
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == ManifestMismatchError.exit_code


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path, responder):
    base_url, _, _ = responder
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    journal = _write_journal(tmp_path / "journal.yaml")
    # Make the write fail by pointing `--out` *under* a regular file:
    # `write_response` mkdirs missing parents, but mkdir into a path that
    # already exists as a file raises OSError.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=blocker / "nested" / "response.yaml",
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == WriteFailedError.exit_code


def test_main_responder_http_error_returns_unexpected_but_writes_out(
    monkeypatch, tmp_path, responder, no_responder_retry
):
    """When the Done POST gets an HTTP error, main fails — but the `--out`
    artifact is written before the POST, so it survives the failure."""
    base_url, _, server = responder
    server.next_status = 500
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "response.yaml"
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=out,
        liveness=tmp_path / "live",
        responder_url=f"{base_url}/done",
        journal_file=journal,
    )
    assert main() == UnexpectedError.exit_code
    # --out is written before the POST, so it survives the failure.
    assert out.exists()


def test_main_responder_url_error_returns_unexpected(
    monkeypatch, tmp_path, no_responder_retry
):
    """A URLError (responder unreachable) fails the command."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    journal = _write_journal(tmp_path / "journal.yaml")
    _set_argv(
        monkeypatch,
        request=request_yaml,
        outputs_manifest=manifest,
        state_dir=tmp_path / "state",
        out=tmp_path / "response.yaml",
        liveness=tmp_path / "live",
        # Port 1 is unbound, so the POST raises URLError immediately.
        responder_url="http://127.0.0.1:1/done",
        journal_file=journal,
    )
    assert main() == UnexpectedError.exit_code


def test_responder_http_error_surfaces_response_body(
    monkeypatch, tmp_path, no_responder_retry
):
    """The apiserver's error body (its specific complaint) must reach the
    raised error — otherwise a 400 like 'missing field `status`' bottoms
    out at a bare 'Bad Request' and forces a manual chase."""
    apiserver_body = b'{"error":"missing field `status` at line 1 column 9"}'

    def failing_urlopen(request, *args, **kwargs):
        raise urllib.error.HTTPError(
            request.full_url,
            400,
            "Bad Request",
            {},
            io.BytesIO(apiserver_body),
        )

    monkeypatch.setattr(urllib.request, "urlopen", failing_urlopen)
    journal = _write_journal(tmp_path / "journal.yaml")
    args = argparse.Namespace(
        responder_url="http://127.0.0.1:1/done",
        responder_method="POST",
        responder_authorization="Bearer tok",
        journal_file=str(journal),
    )
    with pytest.raises(urllib.error.HTTPError) as caught:
        command_lib.notify_done(args, context=None)
    rendered = str(caught.value)
    assert "Bad Request" in rendered
    assert apiserver_body.decode("utf-8") in rendered


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["report-completion"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_report_completion"
