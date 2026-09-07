#
# Copyright 2026 Tabsdata Inc.
#

"""run-cron-trigger command.

One-shot entrypoint of the cron-publisher CronJob's main container.
Kubernetes owns the ticking -- the gateway stamps ``spec.schedule`` from
the function's cron mask at create time (see TD-2625) -- so each fire
spawns a fresh pod that runs this command exactly once: it POSTs the
function's execute endpoint with a ``CronTrigger``
``InternalExecutionRequest`` and exits.

Unlike the long-running supervisor cron worker (``td-crontrigger``) and the
manifest-trigger sidecar, there is no loop, no ``request.yaml``, and no
stage: the trigger URL injected via the worker's ``TD_TRIGGER_*`` secret
already encodes the project + function version, so the request body is all
that is needed. Kubernetes re-spawns the pod on the next schedule tick.

apiserver response handling:

  * ``2xx`` -- execution scheduled; exit 0.
  * anything else / connection failure -- raise; ``@command`` reports it to
    the responder and returns a non-zero exit, so the Job is marked failed
    and the missed fire is visible rather than silently dropped.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.runner import (
    _execution_request_body,
    _fire,
    run,
)

__all__ = ["_execution_request_body", "_fire", "run", "main"]

VERSION = "2.1.0"
INFO = (
    "run-cron-trigger — one-shot; POSTs the function execute endpoint once "
    "per CronJob fire and exits"
)
MAN = (
    "run-cron-trigger\n"
    "    --trigger-url <URL | env:VAR | val:LITERAL>\n"
    "    --trigger-method <METHOD | env:VAR | val:LITERAL>\n"
    "    --trigger-authorization <AUTH | env:VAR | val:LITERAL>\n"
    "    ... + standard responder/liveness/log/journal args\n\n"
    "Builds an HttpRequest from the --trigger-* triplet (injected by the "
    "cron-publisher pod profile from the worker's TD_TRIGGER_* secret) and "
    "POSTs a CronTrigger InternalExecutionRequest once. Exits 0 on 2xx; any "
    "other response (or a connection failure) fails the Job so the missed "
    "fire is visible. Kubernetes re-spawns the pod on the next schedule tick."
)


def _build_parser():
    parser = build_parser(
        prog="run-cron-trigger",
        description="One-shot cron trigger: POST the function execute endpoint once.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument(
        "--trigger-url",
        required=True,
        help=(
            "Execute endpoint URL. Same env:/val: resolution as the responder "
            "triplet -- the pod profile passes an env:SECRET__TD_TRIGGER_URL "
            "reference injected from the worker's trigger secret."
        ),
    )
    parser.add_argument(
        "--trigger-method",
        required=True,
        help="HTTP method for the execute endpoint. Same env:/val: resolution.",
    )
    parser.add_argument(
        "--trigger-authorization",
        required=True,
        help=(
            "Authorization header for the execute endpoint. Same env:/val: "
            "resolution as the responder triplet."
        ),
    )
    return parser


@command(name="td_run_cron_trigger", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
