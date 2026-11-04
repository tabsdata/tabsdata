#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.options import (  # noqa: E501
    RunStreamSourcePluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.runner import (  # noqa: E501
    StreamSourcePlugin,
)

VERSION = version()
GENERATION = 2
INFORMATION = (
    "stream-source-plugin — long-running; runs the stream publisher's source "
    "plugin until SIGTERM, staging each batch for the manifest trigger"
)
MANUAL = (
    "stream-source-plugin --request <PATH> --bundle-folder <PATH> "
    "--context-folder <PATH> --connection-file <PATH> --liveness <PATH>\n\n"
    "Loads the stream publisher from --bundle-folder, its stream source plugin "
    "from the connectors registry and the connection from --connection-file, "
    "opens the function stage --request names, and runs the plugin until it "
    "returns or SIGTERM or SIGINT turns its context inactive. Each batch the "
    "plugin stages lands in the function stage with a manifest the "
    "manifest-trigger sidecar picks up. On a clean end it records the end in the "
    "journal and reports Done."
)


def build_command_parser() -> ArgumentsParser:
    return RunStreamSourcePluginArguments.parser(
        program="stream-source-plugin",
        description="Run the stream publisher's source plugin.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-p-src-stream", parser=build_command_parser)
def main(arguments: RunStreamSourcePluginArguments) -> int:
    return StreamSourcePlugin(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
