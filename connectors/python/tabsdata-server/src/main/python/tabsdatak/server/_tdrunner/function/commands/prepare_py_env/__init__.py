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
from tabsdatak.server._tdrunner.function.commands.prepare_py_env.arguments import (
    PreparePyEnvArguments,
)


def build_command_parser() -> ArgumentsParser:
    return PreparePyEnvArguments.parser(
        program="prepare-py-env",
        description="Prepare the python environment (not yet implemented).",
    )


def _run() -> int:
    arguments: PreparePyEnvArguments = build_command_parser().parse()
    setup_logging(arguments.logging_configuration)
    raise NotImplementedError


@tabsdata_command(name="td_prepare_py_env")
def main() -> int:
    return run_command(_run)
