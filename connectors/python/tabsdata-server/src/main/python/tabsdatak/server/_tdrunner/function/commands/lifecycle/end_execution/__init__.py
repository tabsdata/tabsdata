#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.lifecycle.end_execution.options import (  # noqa: E501
    EndExecutionArguments,
)
from tabsdatak.server._tdrunner.function.commands.lifecycle.end_execution.runner import (  # noqa: E501
    EndExecution,
)

VERSION = version()
GENERATION = 2
INFORMATION = "end-execution — report the function-run response to the API server"
MANUAL = (
    "end-execution --request <PATH> --outputs-manifest <PATH> "
    "--response <PATH> --journal-file <PATH> --liveness <PATH> "
    "--responder-url <URL> --responder-method <METHOD> "
    "--responder-authorization <AUTH>\n\n"
    "Builds the response from --outputs-manifest: a Data entry with the stats "
    "of every request.output and request.system_output table written, NoData "
    "for the rest. Writes it to --response, stamps the end of the run in "
    "--journal-file and posts a Done report carrying the response."
)


def build_command_parser() -> ArgumentsParser:
    return EndExecutionArguments.parser(
        program="end-execution",
        description="Report the function-run response to the API server.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-end-execution", parser=build_command_parser)
def main(arguments: EndExecutionArguments) -> int:
    return EndExecution(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
