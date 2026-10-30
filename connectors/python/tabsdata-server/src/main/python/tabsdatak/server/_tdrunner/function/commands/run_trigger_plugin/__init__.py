#
#  Copyright 2026 Tabsdata Inc.
#

from tabsdatak._logging import setup_logging
from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import (
    run_command,
    tabsdata_command,
)
from tabsdatak.server._tdrunner.function.commands.run_trigger_plugin.arguments import (
    RunTriggerPluginArguments,
)


def build_command_parser() -> ArgumentsParser:
    return RunTriggerPluginArguments.parser(
        program="run-trigger-plugin",
        description="Run a trigger plugin (not yet implemented).",
    )


def _run() -> int:
    arguments: RunTriggerPluginArguments = build_command_parser().parse()
    setup_logging(arguments.logging_configuration)
    raise NotImplementedError


@tabsdata_command(name="td_run_trigger_plugin")
def main() -> int:
    return run_command(_run)
