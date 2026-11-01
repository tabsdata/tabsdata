#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_function.options import (  # noqa: E501
    InspectFunctionArguments,
)
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_function.runner import (  # noqa: E501
    InspectFunction,
)

VERSION = version()
GENERATION = 2
INFORMATION = "inspect-function — print the function run's environment for diagnosis"
MANUAL = (
    "inspect-function --liveness <PATH> --journal-file <PATH> "
    "--responder-url <URL> --responder-method <METHOD> "
    "--responder-authorization <AUTH>\n\n"
    "Prints the worker, run and decorator of the function run and the resources "
    "of its container to the pod log. It can run at any point of the pod, any "
    "number of times. Troubleshooting only."
)


def build_command_parser() -> ArgumentsParser:
    return InspectFunctionArguments.parser(
        program="inspect-function",
        description="Print the function run's environment for diagnosis.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-inspect-function", parser=build_command_parser)
def main(arguments: InspectFunctionArguments) -> int:
    return InspectFunction(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
