#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.gathering.plan_input.options import (
    PlanInputArguments,
)
from tabsdatak.server._tdrunner.function.commands.gathering.plan_input.runner import (
    PlanInput,
)

VERSION = version()
GENERATION = 2
INFORMATION = "plan-input — write the inputs manifest of the function run"
MANUAL = (
    "plan-input --request <PATH> --inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Writes to --inputs-manifest one entry per request.input table, each "
    "record pointing at the storage location of a table version. Nothing is "
    "copied: the function reads the tables where they are stored."
)


def build_command_parser() -> ArgumentsParser:
    return PlanInputArguments.parser(
        program="plan-input",
        description="Write the inputs manifest of the function run.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-plan-input", parser=build_command_parser)
def main(arguments: PlanInputArguments) -> int:
    return PlanInput(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
