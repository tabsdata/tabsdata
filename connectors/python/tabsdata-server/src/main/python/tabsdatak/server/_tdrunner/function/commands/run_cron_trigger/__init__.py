#
# Copyright 2026 Tabs Data Inc.
#

"""run-cron-trigger command.

One-shot entrypoint of the cron-publisher CronJob's main container.
Kubernetes owns the ticking -- the gateway stamps ``spec.schedule`` from
the function's cron mask at create time (see TD-2625) -- so each fire
spawns a fresh pod that runs this command exactly once: it POSTs the
function's execute endpoint with a ``CronTrigger``
``ExecutionRequestInternal`` and exits.

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
import json
import logging

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
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.error import (
    RunCronTriggerErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "1.0.0"
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
    "POSTs a CronTrigger ExecutionRequestInternal once. Exits 0 on 2xx; any "
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


def _execution_request_body() -> bytes:
    """Build the ``ExecutionRequestInternal`` body for one cron fire.

    Mirrors the legacy supervisor builder
    (``td-crontrigger/src/cli.rs:82-88``): a fresh execution every tick --
    no ``external_uid`` dedup and ``skip_if_running`` false -- tagged
    ``TriggerType::CronTrigger`` (``"CT"``). ``trigger_type`` is required
    (no serde default on the Rust field); the rest carry serde defaults but
    are sent explicitly to match the legacy payload.
    """
    return json.dumps({
        "name": None,
        "function_only": False,
        "skip_if_running": False,
        "external_uid": None,
        "trigger_type": "CT",  # TriggerType::CronTrigger
    }).encode("utf-8")


def _fire(url: str, method: str, authorization: str) -> None:
    """POST the execute endpoint once; raise on non-2xx or connection failure.

    Raises:
        CronTriggerConnectionError (CT_1): the POST could not reach the
            apiserver.
        CronTriggerRejectedError (CT_2): the apiserver returned a non-2xx.
    """
    request = HttpRequest(
        url=url,
        method=method,
        headers={"Authorization": authorization, "Content-Type": "application/json"},
    )
    logger.info("run-cron-trigger: firing execute %s %s", method, url)
    try:
        response = invoke(request, body=_execution_request_body())
    except HttpRequestError as exc:
        raise RunCronTriggerErrorCode.CT_1.exception(url=url, detail=str(exc)) from exc

    if response.ok:
        logger.info(
            "run-cron-trigger: execution scheduled (status=%d)", response.status_code
        )
        return

    body_preview = response.body[:256] if response.body else b""
    raise RunCronTriggerErrorCode.CT_2.exception(
        url=url, status=response.status_code, body=repr(body_preview)
    )


@command(name="td_run_cron_trigger", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    _fire(
        resolve_value(args.trigger_url),
        resolve_value(args.trigger_method),
        resolve_value(args.trigger_authorization),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
