#
# Copyright 2026 Tabs Data Inc.
#

"""prep-upload-state command.

Step 7a of the publisher init-container chain. Reads request.yaml, builds
the transporter copy request that pushes state-out tables from local
disk to cloud storage, and writes that request to ``--out``. Step 7b
(`run-transporter`) consumes it.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function._lib.transporter_request import (
    UnsupportedSchemeError,
    build_upload_state_request,
    write_transporter_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

VERSION = "0.1.0"
INFO = (
    "prep-upload-state — generate the transporter request that pushes "
    "state-out tables to cloud"
)
MAN = (
    "prep-upload-state --request <PATH> --state-dir <PATH> --out <PATH> "
    "--liveness <PATH>\n\n"
    "Reads request.yaml, generates the transporter copy YAML for "
    "uploading the state-out tables, and writes it to --out."
)

EXIT_BAD_REQUEST = 10
EXIT_UNSUPPORTED_SCHEME = 11
EXIT_WRITE_FAILED = 12


def _build_parser():
    parser = build_parser(
        prog="prep-upload-state",
        description="Generate the upload-state transporter request.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help="Local source dir for state-out tables",
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
            copy_req = build_upload_state_request(request, state_dir)
        except UnsupportedSchemeError as exc:
            raise CommandSpecificError(EXIT_UNSUPPORTED_SCHEME, str(exc)) from exc

        try:
            write_transporter_request(copy_req, out_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_WRITE_FAILED, f"failed to write {out_path}: {exc}"
            ) from exc
    return 0


@tabsdata_command(name="td_prep_upload_state")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
