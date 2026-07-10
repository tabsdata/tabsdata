#
# Copyright 2026 Tabs Data Inc.
#

"""prep-upload-state command.

Step 7a of the publisher init-container chain. Reads request.yaml, builds
the transporter copy request that pushes state-out tables from local
disk to cloud storage, and writes that request to ``--out``. Step 7b
(`run-transporter`) consumes it.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.prep_upload_state.runner import run

__all__ = ["run", "main"]

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
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
