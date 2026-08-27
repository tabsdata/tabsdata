#
# Copyright 2026 Tabsdata Inc.
#

"""prep-upload-raw command.

Generates the transporter copy request that persists a publisher's
ingested data — the parquets `stamp-inputs` stamped with the framework's
system columns — from local disk to the run's raw-data location.

Reads:
  * `--request`:           the publisher request.yaml. `info.function_raw`
                           is the destination base; when it is absent the
                           function ingests nothing and the request comes
                           out empty.
  * `--inputs-manifest`:   the enriched manifest emitted by `stamp-inputs`,
                           supplying each stamped parquet and the input
                           index that names it in storage.

Writes a single `--out` YAML that `run-transporter` consumes.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.runner import run

__all__ = ["run", "main"]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "2.0.2"
INFO = (
    "prep-upload-raw — generate the transporter request that persists a "
    "publisher's ingested data"
)
MAN = (
    "prep-upload-raw --request <PATH> --inputs-manifest <PATH> "
    "--out <PATH> --storage-config <PATH> --liveness <PATH>\n\n"
    "Reads the enriched inputs manifest written by stamp-inputs and "
    "builds the transporter copy request that uploads each stamped "
    "parquet to the run's raw-data location, named by its input index. "
    "Functions that ingest nothing produce an empty request."
)


def _build_parser():
    parser = build_parser(
        prog="prep-upload-raw",
        description=("Generate the transporter request that persists ingested data."),
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the enriched inputs manifest written by stamp-inputs",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path where the transporter copy request is written",
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        help="Path to the apiserver storage config (mount option keys)",
    )
    return parser


@command(name="td_prep_upload_raw", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
