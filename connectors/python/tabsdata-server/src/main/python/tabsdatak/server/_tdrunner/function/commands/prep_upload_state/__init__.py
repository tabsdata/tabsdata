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
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_upload_state_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_state.error import (
    PrepUploadStateErrorCode,
)

logger = logging.getLogger(__name__)

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
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


@command(name="td_prep_upload_state", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    storage_config = Path(args.storage_config)

    logger.info("Preparing upload-state transporter request")
    request = load_request_or_raise(request_path, PrepUploadStateErrorCode.PUS_1)
    logger.debug(
        "Request declares %d system_output state slot(s): %s; state_dir=%s",
        len(request.system_output),
        request.system_output,
        state_dir,
    )

    copy_req = build_upload_state_request(request, state_dir, storage_config)
    if not copy_req.pairs:
        logger.debug(
            "Upload-state request has no pairs (no state files present on "
            "disk for any declared system_output slot)"
        )
    else:
        logger.debug(
            "Upload-state copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadStateErrorCode.PUS_3.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Upload-state preparation complete; wrote %s", out_path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
