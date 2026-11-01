#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_instance.options import (  # noqa: E501
    InspectInstanceArguments,
)
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_instance.runner import (  # noqa: E501
    InspectInstance,
)

VERSION = version()
GENERATION = 2
INFORMATION = "inspect-instance — print the instance configuration for diagnosis"
MANUAL = (
    "inspect-instance\n\n"
    "Prints every file of the mounted tabsdata/pods and tabsdata/configs "
    "folders and the resources of its container to the pod log. Takes no "
    "options, reports to no responder. Troubleshooting only."
)


def build_command_parser() -> ArgumentsParser:
    return InspectInstanceArguments.parser(
        program="inspect-instance",
        description="Print the instance configuration for diagnosis.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-inspector", parser=build_command_parser, reports=False)
def main(arguments: InspectInstanceArguments) -> int:
    return InspectInstance(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
