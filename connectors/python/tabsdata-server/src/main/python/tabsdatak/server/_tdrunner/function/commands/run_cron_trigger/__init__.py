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

The POST carries a key drawn once per fire, so a retry whose earlier
answer was lost finds the execution it made: the fire happens exactly once.

apiserver response handling:

  * ``2xx`` -- execution scheduled; exit 0.
  * connection failure, ``5xx``, ``408`` or ``429`` -- retried with the
    function runner's shared policy (``_commons.http.retry``), so a fire
    survives an apiserver restart.
  * anything else, or a transient failure that outlasts the retries --
    raise; ``@command`` reports it to the responder and returns a non-zero
    exit, so the Job is marked failed and the missed fire is visible rather
    than silently dropped.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.options import (
    RunCronTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.runner import (
    _execution_request_body,
    _fire,
    run,
)

__all__ = ["_execution_request_body", "_fire", "run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "run-cron-trigger — one-shot; POSTs the function execute endpoint once "
    "per CronJob fire and exits"
)
MANUAL = (
    "run-cron-trigger\n"
    "    --trigger-url <URL | env:VAR | val:LITERAL>\n"
    "    --trigger-method <METHOD | env:VAR | val:LITERAL>\n"
    "    --trigger-authorization <AUTH | env:VAR | val:LITERAL>\n"
    "    ... + standard responder/liveness/log/journal args\n\n"
    "Builds an HttpRequest from the --trigger-* triplet (injected by the "
    "cron-publisher pod profile from the worker's TD_TRIGGER_* secret) and "
    "POSTs a CronTrigger InternalExecutionRequest, retrying connection "
    "failures, 5xx, 408 and 429. Exits 0 on 2xx; any other response (or a "
    "transient failure that outlasts the retries) fails the Job so the missed "
    "fire is visible. Kubernetes re-spawns the pod on the next schedule tick."
)


def build_command_parser() -> ArgumentsParser:
    return RunCronTriggerArguments.parser(
        program="run-cron-trigger",
        description="One-shot cron trigger: POST the function execute endpoint once.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_cron_trigger", parser=build_command_parser)
def main(arguments: RunCronTriggerArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
