#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.options import (  # noqa: E501
    RunSourcePluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.runner import (
    SourcePlugin,
)

VERSION = version()
GENERATION = 2
INFORMATION = "source-plugin — read a publisher's inputs through its source plugin"
MANUAL = (
    "source-plugin --request <PATH> --bundle-folder <PATH> "
    "--output-folder <PATH> --inputs-manifest <PATH> --state-folder <PATH> "
    "--connection-file <PATH> --liveness <PATH>\n\n"
    "Loads the publisher definition from --bundle-folder and its source plugin "
    "from the connectors registry, then calls read_in() to write parquet files "
    "under --output-folder, and writes --inputs-manifest mapping each function "
    "input, in declaration order, to its files. A stream publisher reads the "
    "manifests its streamer staged instead of its own source. The plugin "
    "context's state is read from the previous run and drained to "
    "--state-folder."
)


def build_command_parser() -> ArgumentsParser:
    return RunSourcePluginArguments.parser(
        program="source-plugin",
        description="Read the publisher's inputs through its source plugin.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-p-src-regular", parser=build_command_parser)
def main(arguments: RunSourcePluginArguments) -> int:
    return SourcePlugin(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
