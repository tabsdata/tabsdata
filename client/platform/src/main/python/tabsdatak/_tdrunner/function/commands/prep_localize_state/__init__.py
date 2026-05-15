#
# Copyright 2026 Tabs Data Inc.
#

"""prep-localize-state command.

Step 4a of the publisher init-container chain. Reads request.yaml, builds
the transporter copy request that pulls input + state-in tables from
cloud storage to local disk, and writes that request to ``--out``. Step
4b (`run-transporter`) consumes it.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function._lib.transporter_request import (
    build_localize_state_request,
    write_transporter_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

VERSION = "0.1.0"
INFO = (
    "prep-localize-state — generate the transporter request that pulls "
    "input + state-in tables to local"
)
MAN = (
    "prep-localize-state --request <PATH> --in-dir <PATH> "
    "--state-dir <PATH> --out <PATH> --liveness <PATH>\n\n"
    "Reads request.yaml, generates the transporter copy YAML for "
    "localising the input and system-input tables, and writes it to --out."
)

# Command-specific exit codes (>= 10).
EXIT_BAD_REQUEST = 10
EXIT_UNSUPPORTED_SCHEME = 11
EXIT_WRITE_FAILED = 12


def _build_parser():
    parser = build_parser(
        prog="prep-localize-state",
        description="Generate the localize-state transporter request.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--in-dir",
        required=True,
        dest="in_dir",
        help="Local destination dir for input tables",
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help="Local destination dir for state-in tables",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path to write the transporter request YAML",
    )
    return parser


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    in_dir = Path(args.in_dir)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        try:
            request = load_request(request_path)
        except (FileNotFoundError, InvalidRequestError) as exc:
            raise CommandSpecificError(
                EXIT_BAD_REQUEST, f"unreadable request.yaml: {exc}"
            ) from exc

        try:
            copy_req = build_localize_state_request(request, in_dir, state_dir)
        except UnsupportedSchemeError as exc:
            raise CommandSpecificError(EXIT_UNSUPPORTED_SCHEME, str(exc)) from exc

        try:
            write_transporter_request(copy_req, out_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_WRITE_FAILED, f"failed to write {out_path}: {exc}"
            ) from exc
    return 0


@tabsdata_command(name="td_prep_localize_state")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
