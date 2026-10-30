#
# Copyright 2026 Tabsdata Inc.
#

"""run-transporter command.

Steps 4b and 7b of the publisher init-container chain. Thin wrapper that
exec's `td_transporter --request <PATH>` while running a liveness ticker.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.run_transporter.options import (
    RunTransporterArguments,
)
from tabsdatak.server._tdrunner.function.commands.run_transporter.runner import (
    run,
)

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = "run-transporter — exec td_transporter with the given request YAML"
MANUAL = (
    "run-transporter --request <PATH> --report <PATH> --liveness <PATH>\n\n"
    "Exec's `td_transporter --request <PATH> --report <PATH>` with "
    "stdout/stderr inherited; returns whatever exit code the binary "
    "returned. The report file must not already exist; the binary "
    "writes its execution report there."
)


def build_command_parser() -> ArgumentsParser:
    return RunTransporterArguments.parser(
        program="run-transporter",
        description="Exec td_transporter with a transporter request YAML.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_transporter", parser=build_command_parser)
def main(arguments: RunTransporterArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
