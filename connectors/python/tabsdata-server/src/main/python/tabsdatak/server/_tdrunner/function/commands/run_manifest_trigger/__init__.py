#
# Copyright 2026 Tabs Data Inc.
#

"""run-manifest-trigger command.

Long-running sidecar entrypoint of the streamer pod. Scans
``<stage>/manifests/`` continuously and POSTs ``fn_trigger`` per
manifest, then drains a deletion queue between polls.

Per the TED's Manifest Trigger Runner section:

  Main Loop -- Do the following in loop:

    * Fetch all manifests under <stage>/manifests/
    * Order chronologically (alphabetical == chronological by filename
      construction; see :func:`stage_manifest.enumerate_pending`)
    * For each manifest:
        * POST the apiserver's function-version /execute route with a
          JSON body ``{"skip_if_running": true, "external_uid": <TS>,
          "trigger_type": "ST", ...}``. This matches the
          ``ExecutionRequestInternal`` shape the supervisor's cron
          trigger already sends, so the apiserver handler accepts
          both orchestrators uniformly.
        * if running (200 OK): run back-off-and-delete (do NOT queue
          this manifest yet -- the function-publisher is still reading;
          wait for the next iteration to see a dedup-reject, then
          delete)
        * if rejected because of external_uid (4xx): queue this
          manifest + its files for deletion, then run
          back-off-and-delete
        * if 5xx / connection error: log warning, leave manifest in
          place; next iteration retries

  Back-Off-And-Delete -- while within the back-off period:

    * Pop one path from the deletion queue and delete it; log
      warnings on failure and continue.

Why deletion is gated on dedup-reject instead of 200 OK:
  When the apiserver returns 200 OK it has *scheduled* the
  function-publisher pod; the pod hasn't read the manifest yet. If
  the sidecar deleted the files immediately, the publisher could
  race against the delete. By waiting for the next sidecar iteration
  to see a dedup-reject (apiserver remembers the external_id), we
  know the trigger was already processed and it's safe to clean up.

Deletion queue is in-memory only (Q3 decision): a pod restart loses
queued deletions, but the next loop iteration rediscovers any
already-triggered manifests via dedup-reject and re-queues them.

Lifecycle (Q5 decision): the sidecar exits when the streamer's main
container exits. K8s native sidecar semantics (init container with
``restartPolicy: Always``) deliver SIGTERM when the pod's main
containers terminate; our handler flips the ``active`` flag and the
loop exits at the next check.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from tabsdatak.server._tdrunner.function._lib.command import (
    build_parser,
    command,
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.http_request import (
    HttpRequest,
    HttpRequestError,
    invoke,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Location,
    load_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    InvalidStageManifestError,
    enumerate_pending,
    external_uid_from_filename,
    load_stage_manifest,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.error import (
    RunManifestTriggerErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "2.0.0"
INFO = (
    "run-manifest-trigger — long-running sidecar; scans manifests/ and "
    "POSTs fn_trigger; cleans up between polls"
)
MAN = (
    "run-manifest-trigger\n"
    "    --request <PATH>\n"
    "    --trigger-url <URL | env:VAR | val:LITERAL>\n"
    "    --trigger-method <METHOD | env:VAR | val:LITERAL>\n"
    "    --trigger-authorization <AUTH | env:VAR | val:LITERAL>\n"
    "    --back-off-secs <INT>\n"
    "    ... + standard responder/liveness/log/journal args\n\n"
    "Reads request.yaml to discover the function_stage URI. Builds "
    "an HttpRequest from the --trigger-* triplet and replays it per "
    "manifest under <stage>/manifests/. On dedup-rejected responses "
    "(4xx) queues the manifest's files + manifest itself for "
    "back-off-and-delete cleanup. Exits when SIGTERM/SIGINT arrives "
    "(K8s native-sidecar shutdown signal)."
)

# Min idle sleep while draining the deletion queue: 0.5s is short
# enough that a tail end of the queue gets popped within the back-off
# window, long enough not to busy-spin when the queue is empty.
_IDLE_SLEEP_SECS = 0.5


def _build_parser():
    parser = build_parser(
        prog="run-manifest-trigger",
        description="Long-running sidecar that drives fn_trigger from manifests.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--trigger-url",
        required=True,
        help=(
            "fn_trigger URL. Same env:/val: resolution as the responder "
            "triplet -- the apiserver typically passes an env:SECRET__... "
            "reference via the pod profile."
        ),
    )
    parser.add_argument(
        "--trigger-method",
        required=True,
        help="HTTP method for fn_trigger. Same env:/val: resolution.",
    )
    parser.add_argument(
        "--trigger-authorization",
        required=True,
        help=(
            "Authorization header for fn_trigger. Same env:/val: "
            "resolution as the responder triplet."
        ),
    )
    parser.add_argument(
        "--back-off-secs",
        dest="back_off_secs",
        type=int,
        default=30,
        help=(
            "Seconds to wait between full manifests/ scans. During the "
            "wait, the sidecar drains queued deletions from prior "
            "iterations. Default 30."
        ),
    )
    return parser


# --- helpers --------------------------------------------------------------


def _resolve_local_stage_path(function_stage: Location) -> Path:
    """``file://`` → local Path. Other schemes raise MT_2.

    The sidecar lives in the same pod as the streamer; we currently
    require both to share a local volume.
    """
    parsed = urlparse(function_stage.uri)
    if parsed.scheme != "file":
        raise RunManifestTriggerErrorCode.MT_2.exception(
            uri=function_stage.uri, scheme=parsed.scheme
        )
    return Path(url2pathname(parsed.path))


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
    stage_root: Path,
    queue: deque[Path],
) -> None:
    """Queue every file referenced by the manifest plus the manifest
    file itself for deletion.

    Files first, manifest last -- if the sidecar crashes mid-drain,
    the next iteration's dedup-reject re-enqueues whatever's still
    on disk, so order is for tidiness rather than correctness. A
    missing or malformed manifest is logged and silently skipped
    (the manifest gets queued anyway; the publisher already saw it).
    """
    try:
        slots = load_stage_manifest(manifest_path)
    except (FileNotFoundError, InvalidStageManifestError) as exc:
        # Malformed / vanished -- can't enumerate inner files, but
        # the manifest file itself still needs to go.
        logger.warning(
            "run-manifest-trigger: could not read %s for deletion enqueue "
            "(%s); queuing manifest only",
            manifest_path,
            exc,
        )
        queue.append(manifest_path)
        return
    for slot in slots:
        for rel_path in slot:
            queue.append(stage_root / rel_path)
    queue.append(manifest_path)


def _trigger_one(
    manifest_path: Path,
    stage_root: Path,
    trigger_request: HttpRequest,
    deletion_queue: deque[Path],
) -> None:
    """POST fn_trigger for one manifest; classify the response.

    Outcomes:
      * 2xx -- triggered; manifest stays in place (next iteration's
        dedup-reject is the cue to delete; see module docstring).
      * 4xx -- apiserver rejected (typically external_uid collision);
        enqueue manifest + files for deletion.
      * 5xx / connection error -- transient; log + leave; next
        iteration retries.
    """
    try:
        external_uid = external_uid_from_filename(manifest_path.name)
    except InvalidStageManifestError as exc:
        logger.warning(
            "run-manifest-trigger: skipping malformed manifest filename %s: %s",
            manifest_path.name,
            exc,
        )
        return

    # ExecutionRequestInternal body shape. Mirrors the cron-trigger
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
        return

    if response.ok:
        logger.info(
            "run-manifest-trigger: triggered manifest %s (status=%d); "
            "deferring deletion until next iteration sees dedup-reject",
            manifest_path.name,
            response.status_code,
        )
        return
    if 400 <= response.status_code < 500:
        logger.info(
            "run-manifest-trigger: manifest %s rejected (status=%d); "
            "queueing %d-file payload for deletion",
            manifest_path.name,
            response.status_code,
            -1,  # exact count logged inside _enqueue_for_deletion via slots read
        )
        _enqueue_for_deletion(manifest_path, stage_root, deletion_queue)
        return
    # 5xx, etc.
    logger.warning(
        "run-manifest-trigger: manifest %s got server error (status=%d); "
        "leaving in place, will retry next iteration",
        manifest_path.name,
        response.status_code,
    )


def _drain_until(
    deadline_monotonic: float,
    deletion_queue: deque[Path],
    is_active: callable,
) -> None:
    """Pop and delete files from ``deletion_queue`` until the deadline
    or shutdown signal arrives.

    Each pop deletes one file. Failures are logged + swallowed (the
    file may have been removed by a parallel actor; the goal is best-
    effort cleanup). When the queue is empty we sleep
    ``_IDLE_SLEEP_SECS`` so the loop doesn't busy-spin.
    """
    while is_active() and time.monotonic() < deadline_monotonic:
        if deletion_queue:
            path = deletion_queue.popleft()
            try:
                path.unlink(missing_ok=True)
                logger.debug("run-manifest-trigger: deleted %s", path)
            except OSError as exc:
                logger.warning(
                    "run-manifest-trigger: could not delete %s: %s",
                    path,
                    exc,
                )
        else:
            time.sleep(
                min(_IDLE_SLEEP_SECS, max(0.0, deadline_monotonic - time.monotonic()))
            )


# --- main -----------------------------------------------------------------


@command(name="td_run_manifest_trigger", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    back_off_secs = int(args.back_off_secs)

    logger.info("Starting manifest-trigger sidecar (back_off_secs=%d)", back_off_secs)
    request = load_request_or_raise(request_path, RunManifestTriggerErrorCode.MT_1)
    stage_root = _resolve_local_stage_path(request.info.function_stage)
    trigger_request = _build_trigger_request(args)
    logger.info(
        "run-manifest-trigger: scanning %s; trigger=%s %s",
        stage_root,
        trigger_request.method,
        trigger_request.url,
    )

    deletion_queue: deque[Path] = deque()
    active_flag = {"on": True}

    def _shutdown(signum, _frame):
        logger.info(
            "run-manifest-trigger: received signal %d; will exit at next "
            "active-flag check",
            signum,
        )
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
        manifests = enumerate_pending(stage_root)
        if manifests:
            logger.info(
                "run-manifest-trigger: scan found %d pending manifest(s)",
                len(manifests),
            )
        for manifest_path in manifests:
            if not is_active():
                break
            _trigger_one(manifest_path, stage_root, trigger_request, deletion_queue)
        if not is_active():
            break
        deadline = time.monotonic() + back_off_secs
        _drain_until(deadline, deletion_queue, is_active)

    logger.info(
        "run-manifest-trigger: shutting down; %d item(s) left in deletion queue",
        len(deletion_queue),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
