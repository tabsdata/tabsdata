#
# Copyright 2026 Tabsdata Inc.
#

"""prep-localize-state command.

Reads request.yaml and writes ``--inputs-manifest``: one entry per
``request.input`` table, each record carrying the table's storage
``location``. The transformer and subscriber paths of `run-function`
consume it and scan the tables where they are stored; the publisher path
overwrites it with the source-plugin-produced manifest. Nothing is copied
locally: state is read where it is stored too (see the state handler).
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.runner import (
    run,
)

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFO = "prep-localize-state — write the inputs manifest of the function run"
MAN = (
    "prep-localize-state --request <PATH> --inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Reads request.yaml and writes an inputs manifest at --inputs-manifest "
    "with one entry per request.input table (records carry each table's "
    "storage location)."
)


def command_parser():
    parser = build_parser(
        program="prep-localize-state",
        description="Write the inputs manifest of the function run.",
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        dest="inputs_manifest",
        help=(
            "Path to write the inputs manifest (one entry per "
            "request.input table, pointing at its storage location)"
        ),
    )
    return parser


@command(name="td_prep_localize_state", parser=command_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
