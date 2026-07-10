#
# Copyright 2026 Tabs Data Inc.
#

"""run-manifest-trigger command.

Long-running sidecar entrypoint of the streamer pod. Walks
``<stage>/manifests/`` oldest-first with an in-memory cursor, POSTs
``fn_trigger`` per manifest, and drains a deletion queue between polls.
Works against a local *or* cloud stage via the resolved ``StageBackend``.

apiserver response semantics (``skip_if_running=true``, gates run in order):

  * ``2xx``  -- newly scheduled.
  * ``425``  -- the function's execution is still running (gate 1,
    ``reject_if_running``). Not done yet; leave it, retry.
  * ``409``  -- the function is idle *and* this ``external_uid`` is held by
    an ended run (gate 2, ``reject_if_external_uid_in_use``). Because
    ``425`` gates first on "running", a ``409`` is a **completion** signal:
    the execution (and its file read) has ended, so the manifest + files
    are safe to delete.

Main loop (per poll), driven by ``next_after(cursor)``:

  * Fetch the oldest manifest after the cursor; POST ``fn_trigger`` with
    ``{"skip_if_running": true, "external_uid": <TS>, "trigger_type": "ST", ...}``
    (the ``ExecutionRequestInternal`` shape the cron trigger also sends).
  * On ``409`` (completed) -> queue the manifest + its files for deletion
    and **advance the cursor past it**, then fetch the next.
  * On ``2xx`` / ``425`` / other transient -> **stop**, leaving the cursor
    parked *before* this manifest so the next poll re-fetches it and waits
    for its ``409``. (Never delete on a non-409: a misconfigured ``403``
    must not burn through batches.)
  * Then drain the deletion queue for one back-off period.

The cursor advances only on completion, so each manifest is re-triggered
across its ``200 -> 425… -> 409`` lifetime; that parking is what gives the
publisher time to consume, and keeps per-poll triggers O(1) (no ``425``
storm). The cursor is in-memory and resets to epoch on restart -- the
already-completed front re-issues a ``409`` and is re-deleted (idempotent).

Lifecycle (Q5 decision): the sidecar exits when the streamer's main
container exits. K8s native sidecar semantics (init container with
``restartPolicy: Always``) deliver SIGTERM when the pod's main
containers terminate; our handler flips the ``active`` flag and the
loop exits at the next check.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.runner import (
    _build_trigger_request,
    _drain_until,
    _enqueue_for_deletion,
    _trigger_one,
    _trigger_pending,
    _TriggerOutcome,
    run,
)

__all__ = [
    "_TriggerOutcome",
    "_build_trigger_request",
    "_drain_until",
    "_enqueue_for_deletion",
    "_trigger_one",
    "_trigger_pending",
    "run",
    "main",
]

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
    "manifest under <stage>/manifests/. On 409 Conflict (apiserver's "
    "external_uid dedup fired) queues the manifest's files + manifest "
    "itself for back-off-and-delete cleanup; on any other 4xx/5xx "
    "leaves the manifest in place + logs a warning. Exits when "
    "SIGTERM/SIGINT arrives (K8s native-sidecar shutdown signal)."
)


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


@command(name="td_run_manifest_trigger", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
