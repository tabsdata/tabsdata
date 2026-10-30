#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.options import (  # noqa: E501
    RunDestinationPluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.runner import (  # noqa: E501
    run,
)

VERSION = version()
GENERATION = 2
INFORMATION = (
    "destination-plugin — hand the outputs the subscriber landed to its "
    "destination plugin"
)
MANUAL = (
    "destination-plugin --request <PATH> --bundle-folder <PATH> "
    "--outputs-manifest <PATH> --connection-file <PATH> --liveness <PATH>\n\n"
    "Loads the subscriber definition from --bundle-folder, its destination "
    "plugin from the connectors registry and the connection from "
    "--connection-file, then hands the plugin the outputs --outputs-manifest "
    "lists in the run's landing area: with a file handoff, write_out() gets "
    "local parquet copies holding only the user's columns. Every output must "
    "hold data, unless none does: then the plugin is not called."
)


def build_command_parser() -> ArgumentsParser:
    return RunDestinationPluginArguments.parser(
        program="destination-plugin",
        description="Hand the landed outputs to the destination plugin.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_destination_plugin", parser=build_command_parser)
def main(arguments: RunDestinationPluginArguments) -> int:
    return run(arguments)


if __name__ == "__main__":
    import sys

    sys.exit(main())
