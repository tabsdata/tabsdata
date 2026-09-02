#
# Copyright 2026 Tabsdata Inc.
#

"""prep-localize-state command.

Step 4a of the function-runner chain. Reads request.yaml, builds the
transporter copy request that pulls input + state-in tables from cloud
storage to local disk, and writes it to ``--out``. Step 4b
(`run-transporter`) consumes it.

Also writes two manifests describing what was localized:

  * ``--inputs-manifest``: one entry per ``request.input`` table, with
    ``files`` pointing at ``<in_dir>/<name>.parquet``. The transformer
    path of `run-function` consumes this; the publisher path
    overwrites it with the source-plugin-produced manifest.
  * ``--state-manifest``: one entry per ``request.system_input``
    table, with ``files`` pointing at ``<state_dir>/<name>.parquet``.
    No consumer reads this in the current chain — it exists so future
    steps (manifest-driven prep-upload-state, source-plugin context
    loader, debugging) have a structured handle on framework state-in.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.runner import (
    _build_inputs_manifest_entries,
    _build_state_manifest_entries,
    run,
)

__all__ = [
    "_build_inputs_manifest_entries",
    "_build_state_manifest_entries",
    "run",
    "main",
]

VERSION = "2.1.0"
INFO = (
    "prep-localize-state — generate the transporter request that pulls "
    "input + state-in tables to local, plus the inputs and state manifests"
)
MAN = (
    "prep-localize-state --request <PATH> --in-dir <PATH> "
    "--state-dir <PATH> --out <PATH> --inputs-manifest <PATH> "
    "--state-manifest <PATH> --liveness <PATH>\n\n"
    "Reads request.yaml, generates the transporter copy YAML for "
    "localising the input and system-input tables (writes to --out). "
    "Writes an inputs manifest at --inputs-manifest with one entry per "
    "request.input table (files at <in_dir>/<name>.parquet), and a "
    "state manifest at --state-manifest with one entry per "
    "request.system_input table (files at <state_dir>/<name>.parquet)."
)


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
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        dest="inputs_manifest",
        help=(
            "Path to write the inputs manifest (one entry per "
            "request.input table, pointing at the localized parquets)"
        ),
    )
    parser.add_argument(
        "--state-manifest",
        required=True,
        dest="state_manifest",
        help=(
            "Path to write the state manifest (one entry per "
            "request.system_input table, pointing at the localized "
            "state-in parquets)"
        ),
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


@command(name="td_prep_localize_state", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
