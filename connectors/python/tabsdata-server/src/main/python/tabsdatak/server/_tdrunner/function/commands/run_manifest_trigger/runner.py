#
# Copyright 2026 Tabsdata Inc.
#

"""run-manifest-trigger runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import tempfile
import time
from collections import deque
from enum import Enum, auto
from pathlib import Path
from urllib.parse import urlparse

from tabsdatak.conn.stream.stage._backend import StageBackend
from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpRequestError,
    invoke,
)
from tabsdatak.server._tdrunner.function._commons.run.shutdown import (
    request_shutdown,
)
from tabsdatak.server._tdrunner.function._commons.solver.arguments import (
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak.server._tdrunner.function._lib.request import (
    load_any_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    stage_backend_from_location,
)
from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    FILES_SUBDIR,
    MANIFESTS_SUBDIR,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    MANIFEST_PREFIX,
    MANIFEST_SUFFIX,
    InvalidStageManifestError,
    external_uid_from_filename,
    load_stage_manifest,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.error import (
    RunManifestTriggerErrorCode,
)

# Glob (relative to the stage root) for the streamer's manifests. The cursor-
# based scan fetches the oldest match after the cursor each step.
_MANIFEST_GLOB = f"{MANIFESTS_SUBDIR}/{MANIFEST_PREFIX}*{MANIFEST_SUFFIX}"

logger = logging.getLogger(__name__)

# Min idle sleep while draining the deletion queue: 0.5s is short
# enough that a tail end of the queue gets popped within the back-off
# window, long enough not to busy-spin when the queue is empty.
_IDLE_SLEEP_SECS = 0.5


# --- helpers --------------------------------------------------------------


def _build_trigger_request(args: argparse.Namespace) -> HttpRequest:
    """Resolve the env:/val: trigger triplet and bake it into a
    reusable :class:`HttpRequest`."""
    url = resolve_value(args.trigger_url)
    method = resolve_value(args.trigger_method)
    auth = resolve_value(args.trigger_authorization)
    return HttpRequest(
        url=url,
        method=method,
        headers={"Authorization": auth},
    )


def _enqueue_for_deletion(
    manifest_path: Path,
    external_id: str,
    backend: StageBackend,
    queue: deque[str],
) -> None:
    """Queue the stage-relative paths to delete for a consumed batch: every
    file the manifest references, the manifest itself, and -- on a local
    filesystem only -- the now-empty ``files/<TS>/`` subdir.

    The queue holds stage-relative rels (not absolute paths); the drain
    routes each through ``backend.delete`` (local unlink/rmdir, cloud object
    delete). Files first, manifest, then the per-batch subdir last so the
    ``rmdir`` only fires once its files have drained. A missing/malformed
    manifest is logged and the manifest rel queued anyway (the publisher
    already consumed it; we just can't enumerate its inner files).

    The ``files/<TS>/`` subdir is enqueued only when the backend is *not* an
    object store: object stores have no directories (the prefix vanishes
    with its last key), so there is nothing to rmdir.
    """
    manifest_rel = f"{MANIFESTS_SUBDIR}/{manifest_path.name}"
    try:
        slots = load_stage_manifest(manifest_path)
    except (FileNotFoundError, InvalidStageManifestError) as exc:
        logger.warning(
            "run-manifest-trigger: could not read %s for deletion enqueue "
            "(%s); queuing manifest only",
            manifest_path,
            exc,
        )
        queue.append(manifest_rel)
        return
    for slot in slots:
        for rel in slot:
            queue.append(rel)
    queue.append(manifest_rel)
    if not backend.is_object_store:
        queue.append(f"{FILES_SUBDIR}/{external_id}")


class _TriggerOutcome(Enum):
    """Result of triggering one manifest, used by :func:`_trigger_pending`
    to decide whether to advance to the next manifest this poll.

    ``DEDUP_DELETED`` and ``SKIPPED`` mean "this manifest needs no
    execution slot" -- the scan may advance to the next candidate.
    ``TRIGGERED`` and ``TRANSIENT`` both mean an execution slot is now
    occupied or unavailable (the function runs one execution at a time),
    so every newer manifest would just get the same ``Busy`` --
    the scan should stop and let the next poll re-read from the oldest.
    """

    DEDUP_DELETED = auto()  # Held / Consumed: files + manifest queued for deletion
    SKIPPED = auto()  # unusable (malformed filename); nothing to trigger
    TRIGGERED = auto()  # Scheduled: execution scheduled
    TRANSIENT = auto()  # Busy / other answer / connection error; retry next poll


# The apiserver's ``TriggerOutcome`` (``td-objects/src/dxo/execution.rs``).
_SERVER_OUTCOMES = {
    "Scheduled": _TriggerOutcome.TRIGGERED,
    "Busy": _TriggerOutcome.TRANSIENT,
    "Held": _TriggerOutcome.DEDUP_DELETED,
    "Consumed": _TriggerOutcome.DEDUP_DELETED,
}


def _server_outcome(body: bytes | None) -> _TriggerOutcome | None:
    try:
        answer = json.loads(body or b"")
    except ValueError:
        return None
    data = answer.get("data") if isinstance(answer, dict) else None
    if not isinstance(data, dict):
        return None
    return _SERVER_OUTCOMES.get(data.get("outcome"))


def _trigger_one(
    manifest_path: Path,
    backend: StageBackend,
    trigger_request: HttpRequest,
    deletion_queue: deque[str],
) -> _TriggerOutcome:
    """POST fn_trigger for one manifest; classify the response.

    Outcomes (see :class:`_TriggerOutcome`):
      * ``Scheduled`` -- ``TRIGGERED``; manifest stays in place (next
        iteration's ``Held`` / ``Consumed`` is the cue to delete; see
        module docstring).
      * ``Held`` / ``Consumed`` -- ``DEDUP_DELETED``; apiserver's
        ``external_uid`` dedup gate fired: the previous iteration's POST
        was accepted, the execution was scheduled, and the
        function-publisher pod will consume (or has consumed) this
        manifest. Safe to delete the manifest + its files.
      * ``Busy`` -- ``TRANSIENT``; the function's execution is still running.
      * Any other status or answer, or a connection error -- ``TRANSIENT``;
        anomalous (bad request shape, auth failure, server error,
        network blip). DO NOT delete: the publisher may never have seen
        the manifest and deleting destroys evidence + data. Leave in
        place; the next iteration retries.
      * Malformed filename -- ``SKIPPED``; can't build the external_uid.

    The narrow ``Held`` / ``Consumed``-only delete fixes a prior bug where any 4xx
    silently deleted the manifest -- in particular, a 403 from a
    misconfigured trigger JWT scope would burn through every batch
    the streamer produced without the streamer or operator noticing.
    """
    try:
        external_uid = external_uid_from_filename(manifest_path.name)
    except InvalidStageManifestError as exc:
        logger.warning(
            "run-manifest-trigger: skipping malformed manifest filename %s: %s",
            manifest_path.name,
            exc,
        )
        return _TriggerOutcome.SKIPPED

    # InternalExecutionRequest body shape. Mirrors the cron-trigger
    # caller at td-crontrigger/src/cli.rs:81-91, which is the only
    # other in-tree builder of this payload — keeps the apiserver
    # handler's calling convention uniform across orchestrators.
    # `trigger_type` is required (no #[serde(default)] on the Rust
    # field); the rest carry serde defaults.
    body = json.dumps({
        "name": None,
        "function_only": False,
        "skip_if_running": True,
        "external_uid": external_uid,
        "trigger_type": "ST",  # TriggerType::StageTrigger
    }).encode("utf-8")
    json_request = HttpRequest(
        url=trigger_request.url,
        method=trigger_request.method,
        headers={**trigger_request.headers, "Content-Type": "application/json"},
    )

    try:
        response = invoke(json_request, body=body)
    except HttpRequestError as exc:
        logger.warning(
            "run-manifest-trigger: fn_trigger connection failure for %s; "
            "leaving in place: %s",
            manifest_path.name,
            exc,
        )
        return _TriggerOutcome.TRANSIENT

    outcome = _server_outcome(response.body) if response.ok else None
    if outcome is _TriggerOutcome.TRIGGERED:
        logger.info(
            "run-manifest-trigger: triggered manifest %s; "
            "deferring deletion until next iteration sees dedup-reject",
            manifest_path.name,
        )
        return outcome
    if outcome is _TriggerOutcome.DEDUP_DELETED:
        logger.info(
            "run-manifest-trigger: manifest %s dedup-rejected; "
            "external_uid already seen, queueing for deletion",
            manifest_path.name,
        )
        _enqueue_for_deletion(manifest_path, external_uid, backend, deletion_queue)
        return outcome
    if outcome is _TriggerOutcome.TRANSIENT:
        logger.info(
            "run-manifest-trigger: manifest %s: function busy; retrying next poll",
            manifest_path.name,
        )
        return outcome
    # Anything else (other 4xx, 5xx, etc.) is anomalous -- leave the
    # manifest in place so the next iteration retries, and log loudly
    # at warning so the operator notices. Body snippet helps debug.
    body_preview = response.body[:256] if response.body else b""
    logger.warning(
        "run-manifest-trigger: manifest %s got unexpected response "
        "(status=%d); leaving in place for retry. Body: %r",
        manifest_path.name,
        response.status,
        body_preview,
    )
    return _TriggerOutcome.TRANSIENT


def _trigger_pending(
    backend: StageBackend,
    cursor: str | None,
    trigger_request: HttpRequest,
    deletion_queue: deque[str],
    is_active: callable,
) -> str | None:
    """Scan manifests oldest-first from ``cursor``, advancing only past
    *completed* (``Held`` / ``Consumed``) or unusable (``SKIPPED``) ones, and
    stop at the first manifest still occupying the execution slot.

    The apiserver gates triggers in order: ``Busy`` while the function's
    execution is still running, then ``Held`` / ``Consumed`` once it has
    ended (see the module docstring). So either is a *completion* signal --
    only then is a manifest safe to delete, and only then does the cursor
    advance past it. ``Scheduled`` or ``Busy``/transient (still running /
    busy) leaves the cursor **parked** before that manifest, so the next
    poll re-fetches it and re-checks for its completion. This pipelines one
    execution per poll (drain the completed front, schedule one new)
    without re-scanning the whole backlog -- per-poll triggers are O(1),
    not O(backlog), so no ``Busy`` storm.

    Returns the advanced cursor. The cursor is in-memory and backend-
    internal (filename for local, last-modified for cloud); on a sidecar
    restart it resets to ``None`` (epoch) and the already-completed front
    answers ``Held`` / ``Consumed`` again and is re-deleted -- idempotent, crash-safe.
    """
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        while is_active():
            found = backend.next_after(_MANIFEST_GLOB, cursor, scratch_dir)
            if found is None:
                return cursor  # nothing newer than the cursor
            manifest_path, next_cursor = found
            outcome = _trigger_one(
                manifest_path, backend, trigger_request, deletion_queue
            )
            if outcome in (_TriggerOutcome.DEDUP_DELETED, _TriggerOutcome.SKIPPED):
                cursor = next_cursor  # completed / unusable -> advance past it
                continue
            return cursor  # TRIGGERED or TRANSIENT -> park
    return cursor


def _drain_until(
    deadline_monotonic: float,
    backend: StageBackend,
    deletion_queue: deque[str],
    is_active: callable,
) -> None:
    """Pop stage-relative rels from ``deletion_queue`` and delete them via
    ``backend.delete`` until the deadline or shutdown signal arrives.

    Each pop deletes one rel: a file, or (local only) the per-batch
    ``files/<TS>/`` directory via ``rmdir`` once its files have drained
    ahead of it. Failures are logged + swallowed -- the path may already be
    gone, a directory may still hold files when a prior drain was cut short,
    or the backend's delete may be unavailable (cloud, pending the
    transporter delete op); the next iteration re-enqueues anything still
    consumed. When the queue is empty we sleep ``_IDLE_SLEEP_SECS`` so the
    loop doesn't busy-spin.
    """
    while is_active() and time.monotonic() < deadline_monotonic:
        if deletion_queue:
            rel = deletion_queue.popleft()
            try:
                backend.delete([rel])
                logger.debug("run-manifest-trigger: deleted %s", rel)
            except (OSError, NotImplementedError) as exc:
                logger.warning(
                    "run-manifest-trigger: could not delete %s: %s",
                    rel,
                    exc,
                )
        else:
            time.sleep(
                min(_IDLE_SLEEP_SECS, max(0.0, deadline_monotonic - time.monotonic()))
            )


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    back_off_secs = int(args.back_off_secs)

    logger.info("Starting manifest-trigger sidecar (back_off_secs=%d)", back_off_secs)
    request = load_any_request_or_raise(request_path, RunManifestTriggerErrorCode.MT_1)
    function_stage = request.info.function_stage
    try:
        backend = stage_backend_from_location(function_stage)
    except UnsupportedSchemeError as exc:
        raise RunManifestTriggerErrorCode.MT_2.exception(
            uri=function_stage.uri, scheme=urlparse(function_stage.uri).scheme
        ) from exc
    trigger_request = _build_trigger_request(args)
    logger.info(
        "run-manifest-trigger: scanning %s; trigger=%s %s",
        function_stage.uri,
        trigger_request.method,
        trigger_request.url,
    )

    deletion_queue: deque[str] = deque()
    cursor: str | None = None
    active_flag = {"on": True}

    def _shutdown(signum, _frame):
        logger.info(
            "run-manifest-trigger: received signal %d; will exit at next "
            "active-flag check",
            signum,
        )
        request_shutdown()
        active_flag["on"] = False

    try:
        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
    except ValueError:
        # Off the main thread (test contexts): signal install isn't
        # supported. Tests drive the active flag manually.
        logger.debug(
            "run-manifest-trigger: not on main thread; skipping signal install"
        )

    is_active = lambda: active_flag["on"]  # noqa: E731

    while is_active():
        cursor = _trigger_pending(
            backend, cursor, trigger_request, deletion_queue, is_active
        )
        if not is_active():
            break
        deadline = time.monotonic() + back_off_secs
        _drain_until(deadline, backend, deletion_queue, is_active)

    logger.info(
        "run-manifest-trigger: shutting down; %d item(s) left in deletion queue",
        len(deletion_queue),
    )
    return 0
