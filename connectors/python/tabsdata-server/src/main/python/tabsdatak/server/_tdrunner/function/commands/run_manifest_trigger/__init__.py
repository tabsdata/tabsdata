#
# Copyright 2026 Tabsdata Inc.
#

"""run-manifest-trigger command.

Long-running sidecar entrypoint of the streamer pod. Walks
``<stage>/manifests/`` oldest-first with an in-memory cursor, POSTs
``fn_trigger`` per manifest, and drains a deletion queue between polls.
Works against a local *or* cloud stage via the resolved ``StageBackend``.

The internal trigger answers 200 with a ``TriggerOutcome``
(``skip_if_running=true``, gates run in order):

  * ``Scheduled`` -- newly scheduled.
  * ``Busy``      -- the function's execution has a run in flight (gate 1,
    ``running_execution``: a physical run Requested, Dispatched or
    Running). Not done yet; leave it, retry.
  * ``Held``      -- this ``external_uid`` is held by an execution whose
    trigger run has not committed (gate 2, ``external_uid_holder``). That
    includes a run not dispatched yet -- it has no physical run, so gate 1
    does not see it -- and a run that is done but not committed, failed or
    on hold. Its run still needs the files; leave it, retry.
  * ``Consumed``  -- the execution holding this ``external_uid`` committed
    its trigger run: the batch is durably published, the **completion**
    signal. The manifest + files are safe to delete.
  * Any other outcome name is one the apiserver grew after this sidecar
    was written. Its meaning for the files is undecided, so the sidecar
    exits with ``MT_3`` instead of keeping or deleting on a guess. A 200
    without a readable outcome at all exits with ``MT_4`` for the same
    reason: a retry cannot mend a changed answer shape.

Main loop (per poll), driven by ``next_after(cursor)``:

  * Fetch the oldest manifest after the cursor; POST ``fn_trigger`` with
    ``{"skip_if_running": true, "external_uid": <TS>, "trigger_type": "ST", ...}``
    (the ``InternalExecutionRequest`` shape the cron trigger also sends).
  * On ``Consumed`` (completed) -> queue the manifest + its files for
    deletion and **advance the cursor past it**, then fetch the next.
  * On ``Scheduled`` / ``Held`` / ``Busy`` / other transient -> **stop**,
    leaving the cursor parked *before* this manifest so the next poll
    re-fetches it and waits for its completion. (Never delete on anything
    else: a misconfigured ``403`` must not burn through batches, and a
    ``Held`` batch's run may not have read its files yet.)
  * Then drain the deletion queue for one back-off period.

The cursor advances only on completion, so each manifest is re-triggered
across its ``Scheduled -> Held/Busy… -> Consumed`` lifetime; that parking
is what gives the publisher time to consume, and keeps per-poll triggers
O(1) (no ``Busy`` storm). A cancelled execution releases its
``external_uid``, so a parked manifest whose run was cancelled is answered
``Scheduled`` again and re-runs from its files. The cursor is in-memory and
resets to epoch on restart -- the already-consumed front answers
``Consumed`` again and is re-deleted (idempotent).

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

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.options import (  # noqa: E501
    RunManifestTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.run_manifest_trigger.runner import (
    TriggerOutcome,
    _build_trigger_request,
    _drain_until,
    _enqueue_for_deletion,
    _ManifestOutcome,
    _trigger_one,
    _trigger_pending,
    run,
)

__all__ = [
    "TriggerOutcome",
    "_ManifestOutcome",
    "_build_trigger_request",
    "_drain_until",
    "_enqueue_for_deletion",
    "_trigger_one",
    "_trigger_pending",
    "run",
    "main",
]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "run-manifest-trigger — long-running sidecar; scans manifests/ and "
    "POSTs fn_trigger; cleans up between polls"
)
MANUAL = (
    "run-manifest-trigger\n"
    "    --request <PATH>\n"
    "    --trigger-url <URL | env:VAR | val:LITERAL>\n"
    "    --trigger-method <METHOD | env:VAR | val:LITERAL>\n"
    "    --trigger-authorization <AUTH | env:VAR | val:LITERAL>\n"
    "    --back-off-seconds <INT>\n"
    "    ... + standard responder/liveness/log/journal args\n\n"
    "Reads request.yaml to discover the function_stage URI. Builds "
    "an HttpRequest from the --trigger-* triplet and replays it per "
    "manifest under <stage>/manifests/. On Consumed (the execution holding "
    "the manifest's external_uid committed) queues the manifest's files + "
    "manifest itself for back-off-and-delete cleanup; on any other answer "
    "leaves the manifest in place for the next poll. Exits when "
    "SIGTERM/SIGINT arrives (K8s native-sidecar shutdown signal)."
)


def build_command_parser() -> ArgumentsParser:
    return RunManifestTriggerArguments.parser(
        program="run-manifest-trigger",
        description="Long-running sidecar that drives fn_trigger from manifests.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_manifest_trigger", parser=build_command_parser)
def main(arguments: RunManifestTriggerArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
