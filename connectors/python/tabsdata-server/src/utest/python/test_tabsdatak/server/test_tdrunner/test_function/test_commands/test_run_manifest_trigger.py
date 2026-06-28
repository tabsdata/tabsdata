#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-manifest-trigger sidecar (cursor-based, backend-routed).

Covers:
  * `_build_trigger_request`: env:/val: resolution, headers built
  * `_enqueue_for_deletion`: queues stage-relative rels (files + manifest +,
    on a local backend, the per-batch dir); empty slots; missing/malformed
  * `_trigger_one`: 200 / 4xx / 5xx / connection error / malformed filename,
    returning the right `_TriggerOutcome`
  * `_trigger_pending`: cursor walk -- advance only on 409 (completed) /
    SKIPPED, park on 200/425/transient; honours the active flag
  * `_drain_until`: deletes rels via the backend (files + empty per-batch
    dirs), handles missing/failed deletes, respects deadline + active flag
  * End-to-end (one consume cycle against a fake apiserver)
"""

from __future__ import annotations

import http.server
import json
import socket
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.stream.stage._backend import StageBackend, _LocalStageBackend
from tabsdatak.server._tdrunner.function._lib.http_request import (
    HttpRequest,
)
from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    files_dir,
    manifests_dir,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    external_uid_from_filename,
    format_manifest_filename,
    write_stage_manifest,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger import (
    _build_trigger_request,
    _drain_until,
    _enqueue_for_deletion,
    _trigger_one,
    _trigger_pending,
    _TriggerOutcome,
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
    assert on the wire shape (a JSON body, no query params)."""

    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            capture.append({
                "path": self.path,
                "body": body,
                "content_type": self.headers.get("Content-Type"),
            })
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    return _H


def _sequenced_handler(statuses: list[int], captured_uids: list):
    """Serve ``statuses`` one per POST (the last value repeats once the
    list is exhausted), recording each request's ``external_uid`` so a
    test can assert exactly which manifests were triggered, in order."""
    seq = list(statuses)

    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                captured_uids.append(json.loads(raw).get("external_uid"))
            except ValueError:
                captured_uids.append(None)
            status = seq.pop(0) if len(seq) > 1 else (seq[0] if seq else 200)
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_a, **_kw):
            pass

    return _H


def _local_backend(stage_root: Path) -> StageBackend:
    """A local-filesystem StageBackend rooted at ``stage_root`` -- what
    ``stage_backend_from_location`` returns for a ``file://`` stage."""
    return _LocalStageBackend(LocalFileSrcConn(base_path=str(stage_root)))


def _write_a_manifest(
    stage_root: Path,
    files: list[list[str]],
    *,
    ts: datetime,
) -> Path:
    """Write a manifest under `<stage_root>/manifests/` and create the
    referenced files at their manifest-relative paths (e.g.
    `files/<TS>/<name>`) so tests have something realistic to delete."""
    files_dir(stage_root).mkdir(parents=True, exist_ok=True)
    for slot in files:
        for rel in slot:
            dst = stage_root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text("parquet-stub")
    return write_stage_manifest(stage_root, files, ts=ts)


def _batch_files(ts: datetime, *names: str) -> list[str]:
    """Build the manifest-relative paths a single batch's files take under
    `files/<TS>/`, matching what `stream_src_ctx.stage` writes."""
    external_id = external_uid_from_filename(format_manifest_filename(ts))
    return [f"files/{external_id}/{name}" for name in names]


def _planted_manifests(stage_root: Path, *tss: datetime) -> list[Path]:
    """Plant one manifest per ``ts`` (each with a single file) and return
    their paths in the order given (callers pass oldest-first)."""
    return [
        _write_a_manifest(stage_root, [_batch_files(ts, "a.parquet")], ts=ts)
        for ts in tss
    ]


def _ts(micros: int) -> datetime:
    return datetime(2026, 6, 16, 10, 30, 12, micros)


# --- error wiring ---------------------------------------------------------


def test_mt_2_resolves_to_unsupported_stage_scheme():
    exc = RunManifestTriggerErrorCode.MT_2.exception(uri="gcs://x", scheme="gcs")
    assert isinstance(exc, UnsupportedStageSchemeError)
    assert exc.exit_code == UnsupportedStageSchemeError.exit_code


def test_mt_1_resolves_to_bad_request():
    from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger import (
        error as _err,
    )

    exc = RunManifestTriggerErrorCode.MT_1.exception(path="/r.yaml", detail="boom")
    assert isinstance(exc, _err.BadRequestError)
    assert exc.exit_code == _err.BadRequestError.exit_code


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


# --- _enqueue_for_deletion (stage-relative rels) --------------------------


def test_enqueue_for_deletion_queues_files_then_manifest_then_dir(tmp_path):
    """Files first, manifest, then the per-batch dir last (local backend)."""
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    slot1 = _batch_files(ts, "b.parquet", "c.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0, slot1], ts=ts)
    external_id = external_uid_from_filename(manifest.name)
    queue: deque[str] = deque()
    _enqueue_for_deletion(manifest, external_id, _local_backend(tmp_path), queue)
    assert list(queue) == [
        slot0[0],
        slot1[0],
        slot1[1],
        f"manifests/{manifest.name}",
        f"files/{external_id}",
    ]


def test_enqueue_for_deletion_missing_manifest_queues_manifest_only(tmp_path):
    missing = (
        manifests_dir(tmp_path) / "stream-manifest-2026-06-16T10-30-12.000001.json"
    )
    queue: deque[str] = deque()
    _enqueue_for_deletion(missing, "eid", _local_backend(tmp_path), queue)
    assert list(queue) == [f"manifests/{missing.name}"]


def test_enqueue_for_deletion_malformed_manifest_queues_manifest_only(tmp_path):
    manifests_dir(tmp_path).mkdir(parents=True, exist_ok=True)
    bad = manifests_dir(tmp_path) / "stream-manifest-2026-06-16T10-30-12.000001.json"
    bad.write_text("not json")
    queue: deque[str] = deque()
    _enqueue_for_deletion(bad, "eid", _local_backend(tmp_path), queue)
    assert list(queue) == [f"manifests/{bad.name}"]


def test_enqueue_for_deletion_empty_slots_queues_manifest_and_dir(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [[], []], ts=ts)
    external_id = external_uid_from_filename(manifest.name)
    queue: deque[str] = deque()
    _enqueue_for_deletion(manifest, external_id, _local_backend(tmp_path), queue)
    assert list(queue) == [f"manifests/{manifest.name}", f"files/{external_id}"]


# --- _trigger_one ---------------------------------------------------------


def test_trigger_one_200_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    queue: deque[str] = deque()
    with _server(_silent_handler(200)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        outcome = _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.TRIGGERED
    assert list(queue) == []


def test_trigger_one_409_enqueues(tmp_path):
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0], ts=ts)
    external_id = external_uid_from_filename(manifest.name)
    queue: deque[str] = deque()
    with _server(_silent_handler(409, body=b'{"error":"seen"}')) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        outcome = _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.DEDUP_DELETED
    assert list(queue) == [
        slot0[0],
        f"manifests/{manifest.name}",
        f"files/{external_id}",
    ]


def test_trigger_one_5xx_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    queue: deque[str] = deque()
    with _server(_silent_handler(503)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        outcome = _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.TRANSIENT
    assert list(queue) == []


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422, 425])
def test_trigger_one_non_409_does_not_enqueue(tmp_path, status):
    """Only 409 (completed) deletes. Everything else -- including 425
    (still running) -- leaves the manifest in place."""
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    queue: deque[str] = deque()
    with _server(_silent_handler(status)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        outcome = _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.TRANSIENT, f"status={status}"
    assert list(queue) == []


def test_trigger_one_connection_failure_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    queue: deque[str] = deque()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    req = HttpRequest(url=f"http://127.0.0.1:{port}/fn_trigger", headers={})
    outcome = _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.TRANSIENT
    assert list(queue) == []


def test_trigger_one_sends_execution_request_internal_body(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [], ts=ts)
    external_id = external_uid_from_filename(manifest.name)
    queue: deque[str] = deque()
    capture: list[dict] = []
    with _server(_request_capturing_handler(200, capture)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_one(manifest, _local_backend(tmp_path), req, queue)
    assert len(capture) == 1
    assert capture[0]["content_type"] == "application/json"
    body = json.loads(capture[0]["body"])
    assert body["external_uid"] == external_id
    assert body["trigger_type"] == "ST"
    assert body["skip_if_running"] is True


def test_trigger_one_malformed_filename_skipped_no_invoke(monkeypatch, tmp_path):
    bogus = manifests_dir(tmp_path) / "junk-file.json"
    bogus.parent.mkdir(parents=True)
    bogus.write_text("[]")
    queue: deque[str] = deque()
    invoked: list[bool] = []

    def _no_call(*_a, **_kw):
        invoked.append(True)
        raise AssertionError("invoke must not run for malformed filenames")

    monkeypatch.setattr(
        "tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.invoke",
        _no_call,
    )
    req = HttpRequest(url="http://x/", headers={})
    outcome = _trigger_one(bogus, _local_backend(tmp_path), req, queue)
    assert outcome == _TriggerOutcome.SKIPPED
    assert invoked == []
    assert list(queue) == []


# --- _trigger_pending (cursor walk) ---------------------------------------


def test_trigger_pending_parks_after_first_triggered(tmp_path):
    """Front manifest schedules (200) -> stop, cursor stays before it (so
    it's re-seen next poll for its 409). Only one manifest is POSTed."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2), _ts(3))
    uids: list[str] = []
    queue: deque[str] = deque()
    with _server(_sequenced_handler([200], uids)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        cursor = _trigger_pending(
            _local_backend(tmp_path), None, req, queue, lambda: True
        )
    assert uids == [external_uid_from_filename(manifests[0].name)]
    assert cursor is None  # parked before M1, did not advance
    assert list(queue) == []


def test_trigger_pending_advances_through_completed_then_parks(tmp_path):
    """Two completed (409) manifests are deleted + the cursor advances past
    them; the third (425, still running) parks the cursor."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2), _ts(3), _ts(4))
    uids: list[str] = []
    queue: deque[str] = deque()
    with _server(_sequenced_handler([409, 409, 425], uids)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        cursor = _trigger_pending(
            _local_backend(tmp_path), None, req, queue, lambda: True
        )
    assert uids == [
        external_uid_from_filename(manifests[0].name),
        external_uid_from_filename(manifests[1].name),
        external_uid_from_filename(manifests[2].name),
    ]
    # cursor advanced past the two completed manifests, parked before M3.
    assert cursor == manifests[1].name
    # M1 + M2 rels enqueued; M3 (425) not.
    assert f"manifests/{manifests[0].name}" in queue
    assert f"manifests/{manifests[1].name}" in queue
    assert f"manifests/{manifests[2].name}" not in queue


def test_trigger_pending_parks_when_oldest_busy(tmp_path):
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2))
    uids: list[str] = []
    queue: deque[str] = deque()
    with _server(_sequenced_handler([425], uids)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        cursor = _trigger_pending(
            _local_backend(tmp_path), None, req, queue, lambda: True
        )
    assert uids == [external_uid_from_filename(manifests[0].name)]
    assert cursor is None
    assert list(queue) == []


def test_trigger_pending_honors_inactive_flag(tmp_path):
    _planted_manifests(tmp_path, _ts(1))
    uids: list[str] = []
    queue: deque[str] = deque()
    with _server(_sequenced_handler([200], uids)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        _trigger_pending(_local_backend(tmp_path), None, req, queue, lambda: False)
    assert uids == []
    assert list(queue) == []


# --- _drain_until ---------------------------------------------------------


def test_drain_until_deletes_queued_rels(tmp_path):
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    _write_a_manifest(tmp_path, [slot0], ts=ts)
    queue: deque[str] = deque([slot0[0]])
    _drain_until(time.monotonic() + 5, _local_backend(tmp_path), queue, lambda: True)
    assert not (tmp_path / slot0[0]).exists()
    assert list(queue) == []


def test_drain_until_rmdirs_empty_batch_dir(tmp_path):
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    _write_a_manifest(tmp_path, [slot0], ts=ts)
    external_id = external_uid_from_filename(format_manifest_filename(ts))
    # file rel ahead of the dir rel, as _enqueue_for_deletion orders them.
    queue: deque[str] = deque([slot0[0], f"files/{external_id}"])
    _drain_until(time.monotonic() + 5, _local_backend(tmp_path), queue, lambda: True)
    assert not (tmp_path / slot0[0]).exists()
    assert not (tmp_path / "files" / external_id).exists()
    assert list(queue) == []


def test_drain_until_handles_missing_rel(tmp_path):
    queue: deque[str] = deque(["files/nope/ghost.parquet"])
    _drain_until(time.monotonic() + 1, _local_backend(tmp_path), queue, lambda: True)
    assert list(queue) == []


def test_drain_until_logs_warning_on_delete_failure(tmp_path, caplog):
    """A non-empty dir rel fails rmdir -- logged + swallowed, loop moves on."""
    (tmp_path / "files" / "x").mkdir(parents=True)
    (tmp_path / "files" / "x" / "leftover.parquet").write_text("x")
    queue: deque[str] = deque(["files/x"])
    with caplog.at_level("WARNING"):
        _drain_until(
            time.monotonic() + 1, _local_backend(tmp_path), queue, lambda: True
        )
    assert list(queue) == []
    assert (tmp_path / "files" / "x").exists()
    assert any("could not delete" in r.message for r in caplog.records)


def test_drain_until_respects_deadline_when_queue_empty(tmp_path):
    queue: deque[str] = deque()
    start = time.monotonic()
    _drain_until(start + 0.3, _local_backend(tmp_path), queue, lambda: True)
    elapsed = time.monotonic() - start
    assert 0.25 <= elapsed <= 0.8


def test_drain_until_exits_immediately_when_inactive(tmp_path):
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    _write_a_manifest(tmp_path, [slot0], ts=ts)
    queue: deque[str] = deque([slot0[0]])
    _drain_until(time.monotonic() + 10, _local_backend(tmp_path), queue, lambda: False)
    assert (tmp_path / slot0[0]).exists()  # never entered the loop
    assert list(queue) == [slot0[0]]


# --- end-to-end (one consume cycle) ---------------------------------------


def test_consume_cycle_triggers_then_drains(tmp_path):
    """Two completed manifests (409): _trigger_pending enqueues both + their
    files, advancing the cursor; _drain_until then removes them all."""
    backend = _local_backend(tmp_path)
    m1 = _write_a_manifest(tmp_path, [_batch_files(_ts(1), "p1.parquet")], ts=_ts(1))
    m2 = _write_a_manifest(tmp_path, [_batch_files(_ts(2), "p2.parquet")], ts=_ts(2))
    eid1 = external_uid_from_filename(m1.name)
    eid2 = external_uid_from_filename(m2.name)
    queue: deque[str] = deque()
    with _server(_silent_handler(409)) as base_url:
        req = HttpRequest(url=base_url + "/fn_trigger", headers={})
        cursor = _trigger_pending(backend, None, req, queue, lambda: True)
    assert cursor == m2.name  # advanced past both completed manifests
    _drain_until(time.monotonic() + 5, backend, queue, lambda: True)
    assert not m1.exists() and not m2.exists()
    assert not (tmp_path / "files" / eid1).exists()
    assert not (tmp_path / "files" / eid2).exists()
