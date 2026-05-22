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

from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import install_liveness_handler
from tabsdatak._tdrunner.function._lib.outputs_manifest import (
    InvalidOutputsManifestError,
    load_outputs_manifest,
)
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function._lib.transporter_request import (
    OutputTableNotInManifestError,
    build_upload_tables_request,
    write_transporter_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

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

EXIT_BAD_REQUEST = 10
EXIT_BAD_MANIFEST = 11
EXIT_MANIFEST_MISMATCH = 12
EXIT_UNSUPPORTED_SCHEME = 13
EXIT_WRITE_FAILED = 14


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


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    out_path = Path(args.out)
    install_liveness_handler(Path(args.liveness))

    try:
        request = load_request(request_path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise CommandSpecificError(
            EXIT_BAD_REQUEST, f"unreadable request.yaml: {exc}"
        ) from exc

    try:
        manifest_entries = load_outputs_manifest(outputs_manifest_path)
    except (FileNotFoundError, InvalidOutputsManifestError) as exc:
        raise CommandSpecificError(
            EXIT_BAD_MANIFEST,
            f"unreadable outputs manifest: {exc}",
        ) from exc

    try:
        copy_req = build_upload_tables_request(request, manifest_entries)
    except OutputTableNotInManifestError as exc:
        raise CommandSpecificError(EXIT_MANIFEST_MISMATCH, str(exc)) from exc
    except UnsupportedSchemeError as exc:
        raise CommandSpecificError(EXIT_UNSUPPORTED_SCHEME, str(exc)) from exc

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_WRITE_FAILED, f"failed to write {out_path}: {exc}"
        ) from exc
    return 0


@tabsdata_command(name="td_prep_upload_tables")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
