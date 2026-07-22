#
#  Copyright 2026 Tabsdata Inc.
#

from tabsdatak._logging import setup_logging
from tabsdatak.server._tdrunner.function._lib.command import build_parser, run_command
from tabsdatak.server._tdrunner.function.commands import tabsdata_command


def _build_parser():
    return build_parser(
        prog="prepare-py-env",
        description="Prepare the python environment (not yet implemented).",
    )


def _run() -> int:
    args = _build_parser().parse_args()
    setup_logging(args.log_config)
    raise NotImplementedError


@tabsdata_command(name="td_prepare_py_env")
def main() -> int:
    return run_command(_run)
