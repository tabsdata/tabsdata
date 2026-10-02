#
# Copyright 2026 Tabsdata Inc.
#

"""stamp-inputs command.

Follows `run-source-plugin` in the publisher chain. Reads the raw
parquet files it produced, stamps each one with the framework's system
columns (per-row UUID identifier plus execution/transaction/version/
timestamp metadata broadcast from the request envelope), and writes the
enriched parquets under `--enriched-dir`. Emits an updated
inputs-manifest at `--enriched-inputs-manifest`, which `prep-upload-raw`
uses to persist the stamped parquets and `run-function` to consume
them.

Legacy counterpart: `tabsdata._tabsserver.function.execution_utils.
store_source_raw_data` — the legacy `_tabsserver` ran this in-process
as part of the function-execution wrapper. We extract it into its own
runner step so the data-shape boundary between "plugin output" and
"framework-managed input to user function" is explicit.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.runner import (
    _stamp_file,
    run,
)

__all__ = ["_stamp_file", "run", "main"]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "2.2.0"
INFO = (
    "stamp-inputs — stamp framework system columns onto the parquets "
    "produced by run-source-plugin"
)
MAN = (
    "stamp-inputs --request <PATH> --inputs-manifest <PATH> "
    "--enriched-dir <PATH> --enriched-inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the raw inputs manifest, stamps each parquet with the "
    "framework's system columns ($td.id per row plus $td.ver.execution"
    " / .transaction / .version / .timestamp broadcast across rows), "
    "writes enriched parquets under --enriched-dir, and emits an "
    "updated inputs manifest at --enriched-inputs-manifest for "
    "run-function to consume."
)


def _build_parser():
    parser = build_parser(
        program="stamp-inputs",
        description=(
            "Stamp framework system columns onto run-source-plugin's parquets."
        ),
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the YAML inputs manifest written by run-source-plugin",
    )
    parser.add_argument(
        "--enriched-dir",
        required=True,
        help="Directory where stamped parquets are written",
    )
    parser.add_argument(
        "--enriched-inputs-manifest",
        required=True,
        help=(
            "Path where the enriched inputs manifest is written "
            "(consumed by run-function)"
        ),
    )
    return parser


@command(name="td_stamp_inputs", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
