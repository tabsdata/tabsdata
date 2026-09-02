#
# Copyright 2026 Tabsdata Inc.
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

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.prep_upload_tables.runner import run

__all__ = ["run", "main"]

VERSION = "2.1.0"
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
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


@command(name="td_prep_upload_tables", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
