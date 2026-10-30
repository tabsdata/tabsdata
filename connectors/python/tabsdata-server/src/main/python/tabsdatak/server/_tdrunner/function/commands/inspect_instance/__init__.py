#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import (
    tabsdata_command,
)
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.inspect_instance.arguments import (
    InspectInstanceArguments,
)
from tabsdatak.server._tdrunner.function.commands.inspect_instance.runner import run

__all__ = ["main"]

VERSION = version()
GENERATION = 1


def build_command_parser() -> ArgumentsParser:
    return InspectInstanceArguments.parser(
        program="inspect-instance",
        description="Dump mounted instance config and pod folders for troubleshooting.",
        version=VERSION,
    )


def command(*, name: str):
    def wrap(function):
        return tabsdata_command(name=name)(function)

    return wrap


@command(name="td_inspector")
def main() -> int:
    arguments: InspectInstanceArguments = build_command_parser().parse()
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
