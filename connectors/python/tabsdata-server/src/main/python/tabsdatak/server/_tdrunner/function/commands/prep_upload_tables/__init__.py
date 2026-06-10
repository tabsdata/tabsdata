#
# Copyright 2026 Tabs Data Inc.
#

"""prep-upload-tables command.

Generates the transporter copy request that uploads the user-facing
output tables produced by `run-function` from local disk to the
destinations declared in `request.yaml`.

Reads:
  * `--request`:           the publisher request.yaml (drives the
                           per-table target Location).
  * `--outputs-manifest`:  the YAML manifest emitted by `run-function`,
                           mapping each declared output table to either
                           the absolute parquet path the function wrote
                           or `null` (function returned `None` for that
                           table — nothing to upload).

Writes a single `--out` YAML that `run-transporter` consumes. Tables
the user did not modify (manifest entry with `file: null`) and tables
with no apiserver-allocated `Location` are silently skipped.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    load_outputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter_request import (
    OutputTableNotInManifestError,
    build_upload_tables_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_tables.error import (
    PrepUploadTablesErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "0.1.0"
INFO = (
    "prep-upload-tables — generate the transporter request that pushes "
    "the user-function's output tables to their declared destinations"
)
MAN = (
    "prep-upload-tables --request <PATH> --outputs-manifest <PATH> "
    "--out <PATH> --liveness <PATH>\n\n"
    "Reads request.yaml and the outputs manifest produced by "
    "run-function, generates the transporter copy YAML that uploads "
    "each modified output table to the Location declared in "
    "request.yaml, and writes it to --out. Tables the user function "
    "did not modify (manifest entry with file: null) and tables with "
    "no apiserver-allocated Location are skipped."
)


def _build_parser():
    parser = build_parser(
        prog="prep-upload-tables",
        description="Generate the upload-tables transporter request.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        dest="outputs_manifest",
        help="Path to the outputs manifest YAML produced by run-function",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path to write the transporter request YAML",
    )
    return parser


@command(name="td_prep_upload_tables", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    out_path = Path(args.out)

    logger.info("Preparing upload-tables transporter request")
    request = load_request_or_raise(request_path, PrepUploadTablesErrorCode.PUT_1)
    manifest_entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, PrepUploadTablesErrorCode.PUT_2
    )
    logger.debug(
        "Outputs manifest has %d entr%s: %s (from %s); "
        "request declares %d output table(s): %s",
        len(manifest_entries),
        "y" if len(manifest_entries) == 1 else "ies",
        manifest_entries,
        outputs_manifest_path,
        len(request.output),
        request.output,
    )

    try:
        copy_req = build_upload_tables_request(request, manifest_entries)
    except OutputTableNotInManifestError as exc:
        raise PrepUploadTablesErrorCode.PUT_3.exception(cause=exc, detail=str(exc))
    except UnsupportedSchemeError as exc:
        raise PrepUploadTablesErrorCode.PUT_4.exception(cause=exc, detail=str(exc))
    if not copy_req.pairs:
        logger.debug(
            "Upload-tables request has no pairs (every declared output table "
            "either had file: null in the manifest or no Location)"
        )
    else:
        logger.debug(
            "Upload-tables copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadTablesErrorCode.PUT_5.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Upload-tables preparation complete; wrote %s", out_path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
