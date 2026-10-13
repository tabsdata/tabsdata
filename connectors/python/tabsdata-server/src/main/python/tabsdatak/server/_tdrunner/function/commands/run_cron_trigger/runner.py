#
# Copyright 2026 Tabsdata Inc.
#

"""run-cron-trigger runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import json
import logging
import uuid

from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpRequestError,
    invoke,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
from tabsdatak.server._tdrunner.function._commons.solver.arguments import resolve_value
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.error import (
    RunCronTriggerErrorCode,
)

logger = logging.getLogger(__name__)


def _execution_request_body(external_uid: str) -> bytes:
    """Build the ``InternalExecutionRequest`` body for one cron fire,
    tagged ``TriggerType::CronTrigger`` (``"CT"``) and keyed by
    ``external_uid`` so a retried POST whose answer was lost finds the
    execution it made instead of making a second one. ``trigger_type`` is
    required (no serde default on the Rust field).
    """
    return json.dumps({
        "name": None,
        "function_only": False,
        "skip_if_running": False,
        "external_uid": external_uid,
        "trigger_type": "CT",  # TriggerType::CronTrigger
    }).encode("utf-8")


def _fire(url: str, method: str, authorization: str) -> None:
    """POST the execute endpoint, retrying transient failures with one key,
    so the fire happens exactly once; raise on a non-2xx or a connection
    failure that outlasts the retries. Its CronJob runs one pod per fire, so
    a key drawn here covers every retry of the fire.

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
    external_uid = f"cron-{uuid.uuid4()}"
    logger.info(
        "run-cron-trigger: firing execute %s %s (%s)", method, url, external_uid
    )
    try:
        response = call_with_retry(
            invoke, request, body=_execution_request_body(external_uid)
        )
    except HttpRequestError as exc:
        raise RunCronTriggerErrorCode.CT_1.exception(url=url, detail=str(exc)) from exc

    if response.ok:
        logger.info(
            "run-cron-trigger: execution scheduled (status=%d)", response.status
        )
        return

    body_preview = response.body[:256] if response.body else b""
    raise RunCronTriggerErrorCode.CT_2.exception(
        url=url, status=response.status, body=repr(body_preview)
    )


def run(args: argparse.Namespace) -> int:
    _fire(
        resolve_value(args.trigger_url),
        resolve_value(args.trigger_method),
        resolve_value(args.trigger_authorization),
    )
    return 0
