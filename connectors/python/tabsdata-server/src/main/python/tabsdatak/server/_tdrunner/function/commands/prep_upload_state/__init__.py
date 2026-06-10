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

import argparse
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter_request import (
    build_upload_state_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_state.error import (
    PrepUploadStateErrorCode,
)

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


@command(name="td_prep_upload_state", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)

    request = load_request_or_raise(request_path, PrepUploadStateErrorCode.PUS_1)

    try:
        copy_req = build_upload_state_request(request, state_dir)
    except UnsupportedSchemeError as exc:
        raise PrepUploadStateErrorCode.PUS_2.exception(cause=exc, detail=str(exc))

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadStateErrorCode.PUS_3.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
