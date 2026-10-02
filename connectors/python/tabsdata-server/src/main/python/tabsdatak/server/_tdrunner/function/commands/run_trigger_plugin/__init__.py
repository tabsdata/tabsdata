#
#  Copyright 2026 Tabsdata Inc.
#

from tabsdatak._logging import setup_logging
from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    run_command,
)
from tabsdatak.server._tdrunner.function.commands import tabsdata_command


def _build_parser():
    return build_parser(
        program="run-trigger-plugin",
        description="Run a trigger plugin (not yet implemented).",
    )


def _run() -> int:
    args = _build_parser().parse_args()
    setup_logging(args.log_config)
    raise NotImplementedError


@tabsdata_command(name="td_run_trigger_plugin")
def main() -> int:
    return run_command(_run)
