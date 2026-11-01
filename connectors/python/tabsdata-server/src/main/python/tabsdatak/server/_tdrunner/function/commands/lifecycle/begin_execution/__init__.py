#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.lifecycle.begin_execution.options import (  # noqa: E501
    BeginExecutionArguments,
)
from tabsdatak.server._tdrunner.function.commands.lifecycle.begin_execution.runner import (  # noqa: E501
    BeginExecution,
)

VERSION = version()
GENERATION = 2
INFORMATION = "begin-execution — record the start of the run and report it running"
MANUAL = (
    "begin-execution --journal-file <PATH> --liveness <PATH> "
    "--responder-url <URL> --responder-method <METHOD> "
    "--responder-authorization <AUTH>\n\n"
    "First step of the function run. Writes the start time of the run to "
    "--journal-file and posts a Running report to the responder."
)


def build_command_parser() -> ArgumentsParser:
    return BeginExecutionArguments.parser(
        program="begin-execution",
        description="Record the start of the run and report it running.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-begin-execution", parser=build_command_parser)
def main(arguments: BeginExecutionArguments) -> int:
    return BeginExecution(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
