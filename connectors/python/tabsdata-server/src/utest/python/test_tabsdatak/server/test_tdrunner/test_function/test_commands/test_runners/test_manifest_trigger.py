#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import http.server
import json
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage
from tabsdatak.server._tdrunner.function._handlers import trigger
from tabsdatak.server._tdrunner.function._handlers.sweeper import Sweeper
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger import (
    runner,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.error import (  # noqa: E501
    BadRequestError,
    MalformedTriggerAnswerError,
    RunManifestTriggerErrorCode,
    UnknownTriggerOutcomeError,
    UnsupportedStageSchemeError,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.options import (  # noqa: E501
    RunManifestTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.runner import (  # noqa: E501
    ManifestOutcome,
    ManifestTrigger,
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


def _outcome(name: str) -> bytes:
    """The internal trigger's 200 body naming a ``TriggerOutcome``."""
    return json.dumps({"data": {"outcome": name}}).encode("utf-8")


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


def _request_capturing_handler(capture: list[dict]):
    """Captures the request path, body, and Content-Type so tests can
    assert on the wire shape (a JSON body, no query params); answers
    ``Scheduled``."""
    answer = _outcome("Scheduled")

    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            capture.append({
                "path": self.path,
                "body": body,
                "content_type": self.headers.get("Content-Type"),
            })
            self.send_response(200)
            self.send_header("Content-Length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

        def log_message(self, *_a, **_kw):
            pass

    return _H


def _sequenced_handler(answers: list[str | int], captured_uids: list):
    """Serve ``answers`` one per POST (the last value repeats once the
    list is exhausted): an outcome name as a 200, an int as a bare status.
    Records each request's ``external_uid`` so a
    test can assert exactly which manifests were triggered, in order."""
    seq = list(answers)

    class _H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                captured_uids.append(json.loads(raw).get("external_uid"))
            except ValueError:
                captured_uids.append(None)
            answer = seq.pop(0) if len(seq) > 1 else seq[0]
            status, body = (
                (200, _outcome(answer)) if isinstance(answer, str) else (answer, b"")
            )
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, *_a, **_kw):
            pass

    return _H


def manifest_trigger(root: Path, url: str = "http://127.0.0.1:1/fn_trigger"):
    command = ManifestTrigger(
        RunManifestTriggerArguments(
            liveness=Path("/l"),
            responder_url="val:http://r",
            responder_method="val:POST",
            responder_authorization="val:Bearer r",
            journal_file=Path("/j"),
            request=root / "request.yaml",
            trigger_url=url,
            trigger_method="POST",
            trigger_authorization="Bearer t",
        )
    )
    command.stage = Stage(Location(uri=root.as_uri()))
    command.sweeper = Sweeper(command.stage)
    return command


def queued(command: ManifestTrigger) -> list[str]:
    return list(command.sweeper.queue)


def _write_a_manifest(
    stage_root: Path,
    files: list[list[str]],
    *,
    ts: datetime,
) -> str:
    for slot in files:
        for name in slot:
            file = stage_root / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("parquet-stub")
    path = StageManifest(ts).path
    (stage_root / path).parent.mkdir(parents=True, exist_ok=True)
    (stage_root / path).write_text(json.dumps(files))
    return path


def _uid(manifest: str) -> str:
    return StageManifest.parse(manifest).batch


def _batch_files(ts: datetime, *names: str) -> list[str]:
    manifest = StageManifest(ts)
    return [manifest.file(name) for name in names]


def _planted_manifests(stage_root: Path, *tss: datetime) -> list[str]:
    return [
        _write_a_manifest(stage_root, [_batch_files(ts, "a.parquet")], ts=ts)
        for ts in tss
    ]


def _ts(micros: int) -> datetime:
    return datetime(2026, 6, 16, 10, 30, 12, micros)


def _unused_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


# --- error wiring ---------------------------------------------------------


def test_mt_2_resolves_to_unsupported_stage_scheme():
    exc = RunManifestTriggerErrorCode.MT_2.exception(uri="gcs://x", scheme="gcs")
    assert isinstance(exc, UnsupportedStageSchemeError)
    assert exc.exit_code == UnsupportedStageSchemeError.exit_code


def test_mt_1_resolves_to_bad_request():
    exc = RunManifestTriggerErrorCode.MT_1.exception(path="/r.yaml", detail="boom")
    assert isinstance(exc, BadRequestError)
    assert exc.exit_code == BadRequestError.exit_code


def test_a_stage_no_store_supports_is_refused(tmp_path):
    request = tmp_path / "request.yaml"
    request.write_text(
        json.dumps({
            "Streamer": {
                "info": {
                    "function_bundle": {"uri": "file:///bundle.tar.zst"},
                    "function_stage": {"uri": "ftp://bucket/stage"},
                }
            }
        })
    )
    command = manifest_trigger(tmp_path)
    command.request_file = request
    with pytest.raises(UnsupportedStageSchemeError):
        command.load()


# --- trigger_manifest -----------------------------------------------------


def test_trigger_one_scheduled_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    with _server(_silent_handler(200, body=_outcome("Scheduled"))) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        outcome = command.trigger_manifest(manifest)
    assert outcome == ManifestOutcome.TRIGGERED
    assert queued(command) == []


def test_trigger_one_consumed_enqueues(tmp_path):
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0], ts=ts)
    with _server(_silent_handler(200, body=_outcome("Consumed"))) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        outcome = command.trigger_manifest(manifest)
    assert outcome == ManifestOutcome.CONSUMED
    assert queued(command) == [slot0[0], manifest]


def test_trigger_one_held_does_not_enqueue(tmp_path):
    """Held: an execution holds the key but has not committed -- it may not
    have been dispatched yet, or may have failed -- so its run still needs
    the files."""
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0], ts=ts)
    with _server(_silent_handler(200, body=_outcome("Held"))) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        outcome = command.trigger_manifest(manifest)
    assert outcome == ManifestOutcome.HELD
    assert queued(command) == []
    assert (tmp_path / manifest).exists()
    assert (tmp_path / slot0[0]).exists()


def test_trigger_one_unknown_outcome_raises_mt_3(tmp_path):
    """An outcome name the sidecar does not know is a TriggerOutcome variant
    added on the apiserver side: neither keep nor delete is decided, so the
    sidecar stops instead of guessing."""
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0], ts=ts)
    with _server(_silent_handler(200, body=_outcome("Parked"))) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        with pytest.raises(UnknownTriggerOutcomeError, match="'Parked'"):
            command.trigger_manifest(manifest)
    assert queued(command) == []
    assert (tmp_path / manifest).exists()
    assert (tmp_path / slot0[0]).exists()


@pytest.mark.parametrize(
    "body", [b"", b"not json", b'{"data": {}}', b'{"data": {"outcome": 7}}']
)
def test_trigger_one_unreadable_200_raises_mt_4(tmp_path, body):
    """A 200 is the apiserver's own answer, so a body without a readable
    data.outcome is a protocol mismatch a retry cannot mend."""
    ts = _ts(345_000)
    slot0 = _batch_files(ts, "a.parquet")
    manifest = _write_a_manifest(tmp_path, [slot0], ts=ts)
    with _server(_silent_handler(200, body=body)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        with pytest.raises(MalformedTriggerAnswerError):
            command.trigger_manifest(manifest)
    assert queued(command) == []
    assert (tmp_path / manifest).exists()
    assert (tmp_path / slot0[0]).exists()


def test_trigger_one_5xx_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    with _server(_silent_handler(503)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        outcome = command.trigger_manifest(manifest)
    assert outcome == ManifestOutcome.TRANSIENT
    assert queued(command) == []


@pytest.mark.parametrize("answer", [400, 401, 403, 404, 410, 422, 425, "Busy"])
def test_trigger_one_not_completed_does_not_enqueue(tmp_path, answer):
    """Only Consumed (completed) deletes. Every non-200 and Busy (still
    running) leaves the manifest in place for the next poll."""
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    with _server(_sequenced_handler([answer], [])) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        outcome = command.trigger_manifest(manifest)
    assert outcome == ManifestOutcome.TRANSIENT, f"answer={answer}"
    assert queued(command) == []


def test_trigger_one_connection_failure_does_not_enqueue(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [_batch_files(ts, "a.parquet")], ts=ts)
    command = manifest_trigger(
        tmp_path,
        f"http://127.0.0.1:{_unused_port()}/fn_trigger",
    )
    assert command.trigger_manifest(manifest) == ManifestOutcome.TRANSIENT
    assert queued(command) == []


def test_trigger_one_sends_execution_request_internal_body(tmp_path):
    ts = _ts(345_000)
    manifest = _write_a_manifest(tmp_path, [], ts=ts)
    capture: list[dict] = []
    with _server(_request_capturing_handler(capture)) as base_url:
        manifest_trigger(tmp_path, base_url + "/fn_trigger").trigger_manifest(manifest)
    assert len(capture) == 1
    assert capture[0]["content_type"] == "application/json"
    body = json.loads(capture[0]["body"])
    assert body["external_uid"] == _uid(manifest)
    assert body["trigger_type"] == "ST"
    assert body["skip_if_running"] is True


def test_trigger_one_malformed_filename_skipped_no_invoke(monkeypatch, tmp_path):
    bogus = "manifests/junk-file.json"
    (tmp_path / "manifests").mkdir()
    (tmp_path / bogus).write_text("[]")
    invoked: list[bool] = []

    def _no_call(*_a, **_kw):
        invoked.append(True)
        raise AssertionError("invoke must not run for malformed filenames")

    monkeypatch.setattr(trigger, "invoke", _no_call)
    command = manifest_trigger(tmp_path)
    assert command.trigger_manifest(bogus) == ManifestOutcome.SKIPPED
    assert invoked == []
    assert queued(command) == []


# --- trigger_pending (offset walk) ----------------------------------------


def test_trigger_pending_parks_after_first_triggered(tmp_path):
    """Front manifest schedules -> stop, offset stays before it (so it's
    re-seen next poll for its completion). Only one manifest is POSTed."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2), _ts(3))
    uids: list[str] = []
    with _server(_sequenced_handler(["Scheduled"], uids)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
    assert uids == [_uid(manifests[0])]
    assert command.offset is None
    assert queued(command) == []


def test_trigger_pending_advances_through_completed_then_parks(tmp_path):
    """Two completed (Consumed) manifests are deleted + the offset advances
    past them; the third (Busy, still running) parks the offset."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2), _ts(3), _ts(4))
    uids: list[str] = []
    with _server(
        _sequenced_handler(["Consumed", "Consumed", "Busy"], uids)
    ) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
    assert uids == [
        _uid(manifests[0]),
        _uid(manifests[1]),
        _uid(manifests[2]),
    ]
    assert command.offset == manifests[1]
    assert manifests[0] in queued(command)
    assert manifests[1] in queued(command)
    assert manifests[2] not in queued(command)


def test_trigger_pending_parks_when_oldest_held(tmp_path):
    """A Held manifest parks the offset before it and is not deleted: the
    batches after it wait for its execution to commit."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2))
    uids: list[str] = []
    with _server(_sequenced_handler(["Held"], uids)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
    assert uids == [_uid(manifests[0])]
    assert command.offset is None
    assert queued(command) == []


def test_trigger_pending_deletes_held_manifest_once_consumed(tmp_path):
    """A manifest answered Held on one poll and Consumed on the next is kept
    by the first and deleted by the second, and the scan then moves on."""
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2))
    uids: list[str] = []
    with _server(
        _sequenced_handler(["Held", "Consumed", "Scheduled"], uids)
    ) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
        assert command.offset is None
        assert queued(command) == []
        command.trigger_pending()
    first, second = (_uid(manifest) for manifest in manifests)
    assert uids == [first, first, second]
    assert command.offset == manifests[0]
    assert manifests[0] in queued(command)
    assert manifests[1] not in queued(command)


def test_trigger_pending_parks_when_oldest_busy(tmp_path):
    manifests = _planted_manifests(tmp_path, _ts(1), _ts(2))
    uids: list[str] = []
    with _server(_sequenced_handler(["Busy"], uids)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
    assert uids == [_uid(manifests[0])]
    assert command.offset is None
    assert queued(command) == []


def test_trigger_pending_honors_inactive_flag(tmp_path):
    _planted_manifests(tmp_path, _ts(1))
    uids: list[str] = []
    with _server(_sequenced_handler(["Scheduled"], uids)) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.active = False
        command.trigger_pending()
    assert uids == []
    assert queued(command) == []


def test_a_shutdown_stops_the_trigger(tmp_path):
    command = manifest_trigger(tmp_path)
    command.shutdown()
    assert command.is_active() is False


def test_a_stop_requested_while_loading_triggers_nothing(tmp_path, monkeypatch):
    command = manifest_trigger(tmp_path)
    monkeypatch.setattr(runner, "listen_for_shutdown", lambda stop: None)

    def load() -> None:
        command.sweeper = Sweeper(Stage(Location(uri=tmp_path.as_uri())))
        command.shutdown()

    def trigger_pending() -> None:
        raise AssertionError("The trigger ran after a stop")

    monkeypatch.setattr(command, "load", load)
    monkeypatch.setattr(command, "trigger_pending", trigger_pending)

    assert command.run() == 0


# --- end-to-end (one consume cycle) ---------------------------------------


def test_consume_cycle_triggers_then_drains(tmp_path):
    """Two completed manifests (Consumed): trigger_pending queues both and
    their files, advancing the offset; the sweep then removes them all."""
    m1 = _write_a_manifest(tmp_path, [_batch_files(_ts(1), "p1.parquet")], ts=_ts(1))
    m2 = _write_a_manifest(tmp_path, [_batch_files(_ts(2), "p2.parquet")], ts=_ts(2))
    with _server(_silent_handler(200, body=_outcome("Consumed"))) as base_url:
        command = manifest_trigger(tmp_path, base_url + "/fn_trigger")
        command.trigger_pending()
    assert command.offset == m2
    command.sweeper.sweep_until(time.monotonic() + 5, command.is_active)
    assert not (tmp_path / m1).exists() and not (tmp_path / m2).exists()
    assert not (tmp_path / "files" / _uid(m1)).exists()
    assert not (tmp_path / "files" / _uid(m2)).exists()


def batch_at(minute: int) -> StageManifest:
    return StageManifest(datetime(2026, 6, 16, 10, minute, 0))


def plant_batch(stage: Path, batch: StageManifest, *, published: bool) -> str:
    file = stage / batch.file("0.0.t")
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text("stub")
    if published:
        (stage / batch.path).parent.mkdir(parents=True, exist_ok=True)
        (stage / batch.path).write_text(json.dumps([[batch.file("0.0.t")]]))
    return batch.file("0.0.t")


def sweeping(tmp_path: Path) -> tuple[ManifestTrigger, Path]:
    command = manifest_trigger(tmp_path)
    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)
    command.stage = Stage(Location(uri=stage.as_uri()))
    command.sweeper = Sweeper(command.stage)
    return command, stage


def test_a_batch_older_than_the_newest_published_one_without_a_manifest_is_swept(
    tmp_path,
):
    command, stage = sweeping(tmp_path)
    abandoned = plant_batch(stage, batch_at(1), published=False)
    published = plant_batch(stage, batch_at(2), published=True)
    open_batch = plant_batch(stage, batch_at(3), published=False)
    command.sweep_abandoned()
    assert queued(command) == [abandoned]
    assert published not in queued(command)
    assert open_batch not in queued(command)


def test_nothing_is_swept_before_a_batch_is_published(tmp_path):
    command, stage = sweeping(tmp_path)
    plant_batch(stage, batch_at(1), published=False)
    command.sweep_abandoned()
    assert queued(command) == []


def test_the_newest_published_batch_is_remembered_once_consumed(tmp_path):
    command, stage = sweeping(tmp_path)
    plant_batch(stage, batch_at(2), published=True)
    command.sweep_abandoned()
    (stage / batch_at(2).path).unlink()
    abandoned = plant_batch(stage, batch_at(1), published=False)
    command.sweep_abandoned()
    assert abandoned in queued(command)


def test_an_abandoned_batch_is_queued_once(tmp_path):
    command, stage = sweeping(tmp_path)
    abandoned = plant_batch(stage, batch_at(1), published=False)
    plant_batch(stage, batch_at(2), published=True)
    command.sweep_abandoned()
    command.sweep_abandoned()
    assert queued(command) == [abandoned]


@pytest.mark.parametrize("enabled", [False, True], ids=["off", "on"])
def test_abandoned_batches_are_swept_only_when_enabled(tmp_path, monkeypatch, enabled):
    monkeypatch.setattr(runner, "SWEEP_ABANDONED_BATCHES", enabled)
    command, stage = sweeping(tmp_path)
    command.back_off_seconds = 0
    abandoned = plant_batch(stage, batch_at(1), published=False)
    plant_batch(stage, batch_at(2), published=True)
    command.sweep()
    assert queued(command) == ([abandoned] if enabled else [])
