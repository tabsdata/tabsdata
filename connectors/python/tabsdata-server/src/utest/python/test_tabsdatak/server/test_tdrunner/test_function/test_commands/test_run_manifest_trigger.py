#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-manifest-trigger sidecar.

Covers:
  * `_resolve_local_stage_path`: file:// accepted, others raise MT_2
  * `_build_trigger_request`: env:/val: resolution, headers built
  * `_enqueue_for_deletion`: full manifest, missing manifest, malformed manifest
  * `_trigger_one`: 200 OK / 4xx / 5xx / connection error / malformed filename
  * `_drain_until`: pops + deletes, handles missing files, respects deadline,
    respects active flag, sleeps on empty queue
  * End-to-end main loop integration (one iteration against a fake apiserver)
"""

from __future__ import annotations

import http.server
import socket
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.http_request import (
    HttpRequest,
    HttpRequestError,
)
from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    files_dir,
    manifests_dir,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    format_manifest_filename,
    write_stage_manifest,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger import (
    _build_trigger_request,
    _drain_until,
    _enqueue_for_deletion,
    _resolve_local_stage_path,
    _trigger_one,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.error import (
    RunManifestTriggerErrorCode,
    UnsupportedStageSchemeError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers --------------------------------------------------------------


@contextmanager
def _server(handler_cls):
    """Stand up a tiny HTTP server on a random port. Yields base_url."""
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


def _silent_handler(status: int, body: bytes = b""):
    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, *_a, **_kw):
            pass

    return _H


def _request_capturing_handler(status: int, capture: list[dict]):
    """Captures the request path, body, and Content-Type so tests can
    assert on the wire shape (now a JSON body, no query params)."""

    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            capture.append(
                {
                    "path": self.path,
                    "body": body,
                    "content_type": self.headers.get("Content-Type"),
                }
            )
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    return _H


def _write_a_manifest(
    stage_root: Path,
    files: list[list[str]],
    *,
    ts: datetime,
) -> Path:
    """Convenience: write a manifest under `<stage_root>/manifests/`
    and create the referenced files under `<stage_root>/files/` so
    tests have something realistic to delete."""
    files_target = files_dir(stage_root)
    files_target.mkdir(parents=True, exist_ok=True)
    for slot in files:
        for rel in slot:
            # rel is "files/<name>" — strip the prefix to compute the real path
            name = rel.split("/", 1)[1] if "/" in rel else rel
            (files_target / name).write_text("parquet-stub")
    return write_stage_manifest(stage_root, files, ts=ts)


# --- _resolve_local_stage_path --------------------------------------------


def test_resolve_local_stage_path_accepts_file_uri(tmp_path):
    p = _resolve_local_stage_path(Location(uri=f"file://{tmp_path}"))
    assert p == tmp_path


@pytest.mark.parametrize("scheme", ["s3", "az", "gcs", "http"])
def test_resolve_local_stage_path_rejects_non_file_schemes(scheme):
    """Cloud schemes will land alongside StreamerPluginCtx's; until
    then, the sidecar can't even start when handed one."""
    with pytest.raises(UnsupportedStageSchemeError, match=scheme):
        _resolve_local_stage_path(Location(uri=f"{scheme}://bucket/stage"))


def test_mt_2_resolves_to_unsupported_stage_scheme():
    exc = RunManifestTriggerErrorCode.MT_2.exception(uri="s3://x", scheme="s3")
    assert isinstance(exc, UnsupportedStageSchemeError)
    assert exc.exit_code == UnsupportedStageSchemeError.exit_code


# --- _build_trigger_request -----------------------------------------------


class _Args:
    """Stub argparse.Namespace for _build_trigger_request."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_build_trigger_request_resolves_literals():
    args = _Args(
        trigger_url="http://apiserver/fn_trigger",
        trigger_method="POST",
        trigger_authorization="Bearer abc",
    )
    req = _build_trigger_request(args)
    assert req.url == "http://apiserver/fn_trigger"
    assert req.method == "POST"
    assert req.headers == {"Authorization": "Bearer abc"}


def test_build_trigger_request_resolves_env_refs(monkeypatch):
    """The pod profile passes ``env:SECRET__TD_FN_TRIGGER_URL`` etc;
    resolve_value reads from the environment at runtime."""
    monkeypatch.setenv("FAKE_URL", "http://from-env/fn_trigger")
    monkeypatch.setenv("FAKE_METHOD", "POST")
    monkeypatch.setenv("FAKE_AUTH", "Bearer from-env")
    args = _Args(
        trigger_url="env:FAKE_URL",
        trigger_method="env:FAKE_METHOD",
        trigger_authorization="env:FAKE_AUTH",
    )
    req = _build_trigger_request(args)
    assert req.url == "http://from-env/fn_trigger"
    assert req.headers["Authorization"] == "Bearer from-env"


# --- _enqueue_for_deletion ------------------------------------------------


def test_enqueue_for_deletion_queues_files_then_manifest(tmp_path):
    """Files-before-manifest order is for tidiness if the sidecar
    crashes mid-drain (the next iteration's dedup-reject re-enqueues
    whatever's still on disk, so order is not load-bearing)."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(
        tmp_path,
        [["files/a.parquet"], ["files/b.parquet", "files/c.parquet"]],
        ts=ts,
    )
    queue: deque[Path] = deque()
    _enqueue_for_deletion(manifest_path, tmp_path, queue)
    queued = list(queue)
    assert queued == [
        tmp_path / "files" / "a.parquet",
        tmp_path / "files" / "b.parquet",
        tmp_path / "files" / "c.parquet",
        manifest_path,
    ]


def test_enqueue_for_deletion_missing_manifest_queues_manifest_only(tmp_path):
    """If the manifest vanished between scan and enqueue, we queue
    the manifest path anyway (deletion will no-op via missing_ok=True
    in the drain step)."""
    missing = tmp_path / "manifests" / "stream-manifest-2026-06-16T10-30-12.345678.json"
    queue: deque[Path] = deque()
    _enqueue_for_deletion(missing, tmp_path, queue)
    assert list(queue) == [missing]


def test_enqueue_for_deletion_malformed_manifest_queues_manifest_only(tmp_path):
    """Corrupt manifests get the same treatment -- we can't enumerate
    inner files but the manifest itself still needs to go (it might
    re-trigger us forever otherwise)."""
    manifests = manifests_dir(tmp_path)
    manifests.mkdir(parents=True, exist_ok=True)
    bad = manifests / "stream-manifest-2026-06-16T10-30-12.345678.json"
    bad.write_text("not json")
    queue: deque[Path] = deque()
    _enqueue_for_deletion(bad, tmp_path, queue)
    assert list(queue) == [bad]


def test_enqueue_for_deletion_empty_slots_just_queues_manifest(tmp_path):
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [[], []], ts=ts)
    queue: deque[Path] = deque()
    _enqueue_for_deletion(manifest_path, tmp_path, queue)
    assert list(queue) == [manifest_path]


# --- _trigger_one ---------------------------------------------------------


def test_trigger_one_200_does_not_enqueue(tmp_path):
    """Deletion is deferred to the next iteration's dedup-reject;
    immediate deletion would race with the function-publisher pod."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [["files/a.parquet"]], ts=ts)
    queue: deque[Path] = deque()
    handler = _silent_handler(200)
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest_path, tmp_path, req, queue)
    assert list(queue) == []


def test_trigger_one_409_enqueues(tmp_path):
    """409 dedup-reject means the apiserver already saw this
    external_id; the function-publisher is done with the manifest --
    safe to delete."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [["files/a.parquet"]], ts=ts)
    queue: deque[Path] = deque()
    handler = _silent_handler(409, body=b'{"error":"external_id seen"}')
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest_path, tmp_path, req, queue)
    assert list(queue) == [
        tmp_path / "files" / "a.parquet",
        manifest_path,
    ]


def test_trigger_one_5xx_does_not_enqueue(tmp_path):
    """Server errors are transient; the manifest stays in place for
    the next iteration to retry."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [["files/a.parquet"]], ts=ts)
    queue: deque[Path] = deque()
    handler = _silent_handler(503)
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest_path, tmp_path, req, queue)
    assert list(queue) == []


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
def test_trigger_one_non_409_4xx_does_not_enqueue(tmp_path, status):
    """Only 409 (apiserver's external_uid dedup) means the function
    was already triggered for this manifest. Other 4xx (bad request,
    auth failure, route gone, etc.) is anomalous -- deleting the
    manifest would silently lose data + evidence. Leave in place so
    the next iteration retries and the operator sees the warning."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [["files/a.parquet"]], ts=ts)
    queue: deque[Path] = deque()
    handler = _silent_handler(status)
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest_path, tmp_path, req, queue)
    assert list(queue) == [], f"status={status} should not enqueue for deletion"


def test_trigger_one_connection_failure_does_not_enqueue(tmp_path):
    """The apiserver is down; same recovery as 5xx -- leave in place
    and retry next iteration."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [["files/a.parquet"]], ts=ts)
    queue: deque[Path] = deque()
    # Port that's free (so urlopen will fail immediately).
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    req = HttpRequest(url=f"http://127.0.0.1:{port}/fn_trigger", headers={})
    _trigger_one(manifest_path, tmp_path, req, queue)
    assert list(queue) == []


def test_trigger_one_sends_execution_request_internal_body(tmp_path):
    """Verify the wire shape of the POST: a JSON
    ExecutionRequestInternal body, no query params. The apiserver's
    /execute handler reads skip_if_running / external_uid /
    trigger_type from this body (see td-objects ExecutionRequestInternal
    and td-apiserver router/internal.rs function_version_execute)."""
    import json

    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    manifest_path = _write_a_manifest(tmp_path, [], ts=ts)
    queue: deque[Path] = deque()
    seen: list[dict] = []
    handler = _request_capturing_handler(200, seen)
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest_path, tmp_path, req, queue)

    assert len(seen) == 1
    captured = seen[0]
    # No query params now -- everything goes in the body.
    assert "?" not in captured["path"]
    assert captured["content_type"] == "application/json"
    payload = json.loads(captured["body"])
    # 345_000 microseconds == "345000" in the canonical filename
    assert payload == {
        "name": None,
        "function_only": False,
        "skip_if_running": True,
        "external_uid": "2026-06-16T10-30-12.345000",
        "trigger_type": "ST",  # TriggerType::StageTrigger
    }


def test_trigger_one_malformed_filename_skipped_no_invoke(monkeypatch, tmp_path):
    """A non-canonical filename in the manifests dir is logged and
    skipped -- we never POST. Guards against a debris file in
    manifests/ knocking the sidecar over."""
    bogus = tmp_path / "manifests" / "junk-file.json"
    bogus.parent.mkdir(parents=True)
    bogus.write_text("[]")
    queue: deque[Path] = deque()

    invoked: list[bool] = []

    def _no_call(*_args, **_kwargs):
        invoked.append(True)
        raise AssertionError("invoke must not run for malformed filenames")

    monkeypatch.setattr(
        "tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.invoke",
        _no_call,
    )
    req = HttpRequest(url="http://x/", headers={})
    _trigger_one(bogus, tmp_path, req, queue)
    assert invoked == []
    assert list(queue) == []


# --- _drain_until ---------------------------------------------------------


def test_drain_until_pops_and_deletes_files(tmp_path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("x")
    b.write_text("y")
    queue = deque([a, b])
    deadline = time.monotonic() + 10
    _drain_until(deadline, queue, lambda: True)
    assert not a.exists()
    assert not b.exists()
    assert list(queue) == []


def test_drain_until_handles_missing_file_via_missing_ok(tmp_path):
    """missing_ok=True so a parallel actor (the function-publisher
    via shutil.move?) can't break the drain step."""
    ghost = tmp_path / "never-existed.txt"
    queue = deque([ghost])
    deadline = time.monotonic() + 1
    _drain_until(deadline, queue, lambda: True)
    assert list(queue) == []


def test_drain_until_respects_deadline_when_queue_empty(tmp_path):
    """An empty queue + a deadline = bounded wait (no infinite loop)."""
    queue: deque[Path] = deque()
    start = time.monotonic()
    deadline = start + 0.3
    _drain_until(deadline, queue, lambda: True)
    elapsed = time.monotonic() - start
    # Should be ~0.3s with a small slack for idle-sleep granularity
    assert 0.25 <= elapsed <= 0.8


def test_drain_until_exits_immediately_when_inactive(tmp_path):
    """The sidecar exits when the streamer's main container exits;
    drain must honour the active flag mid-back-off."""
    a = tmp_path / "a.txt"
    a.write_text("x")
    queue = deque([a])
    deadline = time.monotonic() + 10
    _drain_until(deadline, queue, lambda: False)
    # We never enter the loop, so the file stays put
    assert a.exists()
    assert list(queue) == [a]


def test_drain_until_logs_warning_on_unlink_failure(tmp_path, caplog):
    """Permission failures etc. are logged + swallowed; the loop
    moves on rather than crashing the sidecar."""
    # Make a path that points at a directory; unlink will refuse.
    a_dir = tmp_path / "i-am-a-dir"
    a_dir.mkdir()
    queue = deque([a_dir])
    deadline = time.monotonic() + 1
    _drain_until(deadline, queue, lambda: True)
    # We pop it regardless; the warning was logged
    assert list(queue) == []


# --- End-to-end main loop (one iteration) ---------------------------------


def test_main_loop_one_iteration_processes_and_drains(tmp_path, monkeypatch):
    """Plant two manifests under <stage>/manifests/, run one
    iteration of the body, verify the apiserver got called twice
    and the queue drained the files of the rejected one."""
    stage_root = tmp_path / "stage"
    ts1 = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    ts2 = datetime(2026, 6, 16, 10, 30, 13, 999_000)
    m1 = _write_a_manifest(stage_root, [["files/p1.parquet"]], ts=ts1)
    m2 = _write_a_manifest(stage_root, [["files/p2.parquet"]], ts=ts2)
    queue: deque[Path] = deque()

    # Fake apiserver: 409 for everything (treat as dedup-reject).
    handler = _silent_handler(409)
    with _server(handler) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(m1, stage_root, req, queue)
        _trigger_one(m2, stage_root, req, queue)
    # Both rejected -> both queued; files-then-manifest per call.
    assert list(queue) == [
        stage_root / "files" / "p1.parquet",
        m1,
        stage_root / "files" / "p2.parquet",
        m2,
    ]
    # Drain with plenty of time
    deadline = time.monotonic() + 5
    _drain_until(deadline, queue, lambda: True)
    assert not m1.exists()
    assert not m2.exists()
    assert not (stage_root / "files" / "p1.parquet").exists()
    assert not (stage_root / "files" / "p2.parquet").exists()


# --- error code wiring ----------------------------------------------------


def test_mt_1_resolves_to_bad_request():
    from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger import (
        error as _err,
    )

    exc = RunManifestTriggerErrorCode.MT_1.exception(path="/r.yaml", detail="boom")
    assert isinstance(exc, _err.BadRequestError)
    assert exc.exit_code == _err.BadRequestError.exit_code


# silence unused-import warnings for symbols used only via runtime tracebacks
_ = HttpRequestError
_ = format_manifest_filename
