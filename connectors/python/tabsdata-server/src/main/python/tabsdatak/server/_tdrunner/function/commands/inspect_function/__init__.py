#
# Copyright 2026 Tabsdata Inc.
#

"""inspect-function command.

Troubleshooting probe that dumps relevant execution information to
stdout so it lands in the pod log for diagnosis. Can be inserted at any
point of a function-run pod chain and any number of times. Intended for
development and diagnosis only; production pods should not include it.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.inspect_function.options import (
    InspectFunctionArguments,
)
from tabsdatak.server._tdrunner.function.commands.inspect_function.runner import run

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "inspect-function — dump function-run execution information for troubleshooting"
)
MANUAL = (
    "inspect-function\n"
    "    --shared-folder <PATH>\n"
    "    --work-folder <PATH>\n"
    "    --logging-configuration <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n\n"
    "Dumps relevant execution information for a function-run pod to "
    "stdout. Can be inserted at any point of the pod chain and any "
    "number of times. Troubleshooting only."
)


def build_command_parser() -> ArgumentsParser:
    return InspectFunctionArguments.parser(
        program="inspect-function",
        description="Dump function-run execution information for troubleshooting.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_inspect_function", parser=build_command_parser)
def main(arguments: InspectFunctionArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
