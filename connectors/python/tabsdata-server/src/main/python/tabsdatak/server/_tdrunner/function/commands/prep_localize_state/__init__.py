#
# Copyright 2026 Tabsdata Inc.
#

"""prep-localize-state command.

Reads request.yaml and writes ``--inputs-manifest``: one entry per
``request.input`` table, each record carrying the table's storage
``location``. The transformer and subscriber paths of `function`
consume it and scan the tables where they are stored; the publisher path
overwrites it with the source-plugin-produced manifest. Nothing is copied
locally: state is read where it is stored too (see the state handler).
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.options import (
    PrepLocalizeStateArguments,
)
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.runner import (
    run,
)

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = "prep-localize-state — write the inputs manifest of the function run"
MANUAL = (
    "prep-localize-state --request <PATH> --inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Reads request.yaml and writes an inputs manifest at --inputs-manifest "
    "with one entry per request.input table (records carry each table's "
    "storage location)."
)


def build_command_parser() -> ArgumentsParser:
    return PrepLocalizeStateArguments.parser(
        program="prep-localize-state",
        description="Write the inputs manifest of the function run.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_prep_localize_state", parser=build_command_parser)
def main(arguments: PrepLocalizeStateArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
