#
# Copyright 2026 Tabs Data Inc.
#

"""run-transporter command.

Steps 4b and 7b of the publisher init-container chain. Thin wrapper that
exec's `td_transporter --request <PATH>` while running a liveness ticker.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
    run_transporter,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

VERSION = "0.1.0"
INFO = "run-transporter — exec td_transporter with the given request YAML"
MAN = (
    "run-transporter --request <PATH> --liveness <PATH>\n\n"
    "Exec's `td_transporter --request <PATH>` with stdout/stderr "
    "inherited; returns whatever exit code the binary returned."
)

EXIT_BINARY_MISSING = 10


def _build_parser():
    parser = build_parser(
        prog="run-transporter",
        description="Exec td_transporter with a transporter request YAML.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Path to the transporter request YAML",
    )
    return parser


def _run() -> int:
    args = _build_parser().parse_args()
    request_yaml = Path(args.request)
    liveness = Liveness(Path(args.liveness))
    try:
        return run_transporter(request_yaml, liveness=liveness)
    except TransporterNotFoundError as exc:
        raise CommandSpecificError(
            EXIT_BINARY_MISSING, f"td_transporter not found: {exc}"
        ) from exc


@tabsdata_command(name="td_run_transporter")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
