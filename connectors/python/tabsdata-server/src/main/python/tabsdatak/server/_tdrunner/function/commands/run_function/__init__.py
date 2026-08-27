#
# Copyright 2026 Tabsdata Inc.
#

"""run-function command.

The penultimate step of the function-runner chain. Reads the inputs
manifest, materializes each input slot as a polars `LazyFrame` (or a
list of LazyFrames for ARRAY-mode slots), invokes the user-decorated
function, and sinks each returned `LazyFrame` to a parquet under
`--out-dir`. Emits an outputs manifest at `--outputs-manifest` mapping
each declared output table to the parquet it produced (or `null` if
the function returned `None` for that table — "not modified this run").

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

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.run_function.runner import (
    _FunctionContext,
    _infer_table_modes,
    _invoke_user_function,
    _load_function_def,
    _materialize_input_slot,
    _output_table_names,
    _scan_parquet_or_fail,
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
    "_scan_parquet_or_fail",
    "_sink_outputs",
    "run",
    "main",
]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "2.0.2"
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
    "table to <out-dir>/slot<i>/<table_name>.parquet. Writes an "
    "outputs manifest at --outputs-manifest mapping each output "
    "table to its parquet (or null if the function returned None). "
    "Handles publisherDef, transformerDef, and subscriberDef bundles."
)


def _build_parser():
    parser = build_parser(
        prog="run-function",
        description="Invoke the publisher's user-defined function.",
        version=VERSION,
        info=INFO,
        man=MAN,
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
        help="Directory where the function's output parquets are written",
    )
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        help=(
            "Path where this command writes the YAML outputs manifest "
            "mapping each output table to its parquet file (or null if "
            "the function returned None). Consumed by prep-upload-state."
        ),
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help=(
            "Local directory mirroring the one used by "
            "prep-localize-state / run-source-plugin / "
            "prep-upload-state. When the user function declares a "
            "trailing ``ctx: TrxCtx`` parameter, the runtime hydrates "
            "the ctx scratch from <state-dir>/<system_output[0].name>."
            "parquet (falling back to <system_input[0].name>.parquet) "
            "before invoking the function, and drains it back to "
            "<state-dir>/<system_output[0].name>.parquet afterwards. "
            "Ignored when the user function has no ``ctx`` parameter."
        ),
    )
    return parser


@command(name="td_run_function", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
