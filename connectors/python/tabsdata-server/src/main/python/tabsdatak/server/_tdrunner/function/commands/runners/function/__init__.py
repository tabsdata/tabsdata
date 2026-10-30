#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.function.options import (
    RunFunctionArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.runner import (
    run,
)

VERSION = version()
GENERATION = 2
INFORMATION = (
    "run-function — call the user function on its inputs and write the tables "
    "it returns"
)
MANUAL = (
    "run-function --request <PATH> --bundle-folder <PATH> "
    "--inputs-manifest <PATH> --output-folder <PATH> "
    "--outputs-manifest <PATH> --state-folder <PATH> --liveness <PATH>\n\n"
    "Loads the function definition from --bundle-folder (publisher, stream "
    "publisher, transformer or subscriber), builds each input from "
    "--inputs-manifest (a single-table slot gets one TableFrame, an array "
    "slot a list), calls the user function, and writes every returned table "
    "in one streaming pass: publisher and transformer outputs where the "
    "request stores them, subscriber outputs to the run's landing area for "
    "run-destination-plugin. The run's state is written along with them. "
    "--outputs-manifest gets each written table's location and stats; a "
    "table the function returned as None gets neither."
)


def build_command_parser() -> ArgumentsParser:
    return RunFunctionArguments.parser(
        program="run-function",
        description="Call the user function and write the tables it returns.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_function", parser=build_command_parser)
def main(arguments: RunFunctionArguments) -> int:
    return run(arguments)


if __name__ == "__main__":
    import sys

    sys.exit(main())
