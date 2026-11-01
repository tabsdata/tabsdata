#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.options import (
    RunCronTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.runner import (
    CronTrigger,
)

VERSION = version()
GENERATION = 2
INFORMATION = (
    "cron-trigger — one-shot; posts the function execute endpoint once per "
    "CronJob fire and exits"
)
MANUAL = (
    "cron-trigger --trigger-url <URL | env:VAR | val:LITERAL> "
    "--trigger-method <METHOD | env:VAR | val:LITERAL> "
    "--trigger-authorization <AUTH | env:VAR | val:LITERAL> --liveness <PATH>\n\n"
    "Posts a CronTrigger execution request, keyed once per fire, to the execute "
    "endpoint the cron-publisher pod injects from the worker's TD_TRIGGER_* "
    "secret. Connection failures, 5xx, 408 and 429 are retried with the same "
    "key, so a fire runs exactly once. Exits 0 on 2xx; any other answer, or a "
    "failure that outlasts the retries, fails the Job so the missed fire is "
    "visible. Kubernetes runs the next fire on the next schedule tick."
)


def build_command_parser() -> ArgumentsParser:
    return RunCronTriggerArguments.parser(
        program="cron-trigger",
        description="Post the function execute endpoint once.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-t-src-cron", parser=build_command_parser)
def main(arguments: RunCronTriggerArguments) -> int:
    return CronTrigger(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
