#
# Copyright 2026 Tabsdata Inc.
#

"""run-function command.

The penultimate step of the function-runner chain. Reads the inputs
manifest, materializes each input slot as a polars `LazyFrame` (or a
list of LazyFrames for ARRAY-mode slots), invokes the user-decorated
function, and sinks each returned table in one streaming pass: a
publisher or transformer output straight to the storage location the
request assigns it, a subscriber output to a parquet under `--out-dir`
for the destination plugin. Emits an outputs manifest at
`--outputs-manifest` with the URI and the footer stats of each written
table (neither, if the function returned `None` for that table — "not
modified this run").

The run's state (`<state-dir>/state.parquet`, drained by
run-source-plugin and by the function's ctx) is written to
`request.system_output[0]` in the same streaming pass, and the manifest's
`state` entry carries its URI and stats.

Kind-agnostic: handles `publisherDef`, `transformerDef`, and
`subscriberDef`. The input slot mode (SINGLE vs ARRAY) is inferred
per-parameter from the user function's type annotations.

For publishers and transformers, output table names come from
`info.output_tables`. Subscribers do not declare output table names
— the count is `dest.cardinality(info.destination)` and the names
are synthesised as `["out_0", "out_1", ...]`. Those synthetic names
are only used internally (parquet filenames + outputs-manifest
entry keys); the destination plugin downstream consumes the
parquets positionally.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.function.runner import (
    _FunctionContext,
    _infer_table_modes,
    _invoke_user_function,
    _load_function_def,
    _materialize_input_slot,
    _output_table_names,
    _output_targets,
    _scan_input,
    _sink_outputs,
    run,
)

__all__ = [
    "_FunctionContext",
    "_infer_table_modes",
    "_invoke_user_function",
    "_load_function_def",
    "_materialize_input_slot",
    "_output_table_names",
    "_output_targets",
    "_scan_input",
    "_sink_outputs",
    "run",
    "main",
]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = version()
GENERATION = 1
INFO = (
    "run-function — invoke the decorated function on the prepared inputs "
    "and sink the returned tables to parquet"
)
MAN = (
    "run-function --request <PATH> --bundle-dir <PATH> "
    "--inputs-manifest <PATH> --out-dir <PATH> "
    "--outputs-manifest <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir, materializes each "
    "input slot from --inputs-manifest as a polars LazyFrame "
    "(single-mode slots → one LazyFrame; array-mode slots → list of "
    "LazyFrames), invokes the user function, and sinks each returned "
    "table: publisher and transformer outputs to the storage location "
    "the request assigns them, subscriber outputs to the run's landing "
    "area, <function_landing>/<position>.t. Writes an outputs manifest "
    "at --outputs-manifest with each written table's URI and stats "
    "(neither if the function returned None). Handles publisherDef, "
    "transformerDef, and subscriberDef bundles."
)


def _build_parser():
    parser = build_parser(
        program="run-function",
        description="Invoke the publisher's user-defined function.",
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir", required=True, help="Extracted bundle directory"
    )
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the YAML inputs manifest produced by stamp-inputs",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help=("Working directory of the function context"),
    )
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        help=(
            "Path where this command writes the YAML outputs manifest "
            "with each output table's URI and stats (neither if the "
            "function returned None). Consumed by run-destination-plugin "
            "and report-completion."
        ),
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_folder",
        help=(
            "Local folder holding this run's state, "
            "<state-dir>/state.parquet. When the user function declares a "
            "trailing ``ctx: TransactionCtx`` parameter, the runtime hydrates "
            "the ctx scratch from that file (falling back to "
            "system_input[0]'s storage location) before invoking the "
            "function, and drains it back afterwards. The file, when "
            "present, is written to system_output[0]'s storage location."
        ),
    )
    return parser


@command(name="td_run_function", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
