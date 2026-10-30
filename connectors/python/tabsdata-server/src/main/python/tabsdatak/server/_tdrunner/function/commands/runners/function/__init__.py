#
# Copyright 2026 Tabsdata Inc.
#

"""run-function command.

The penultimate step of the function-runner chain. Reads the inputs
manifest, materializes each input slot as a polars `LazyFrame` (or a
list of LazyFrames for ARRAY-mode slots), invokes the user-decorated
function, and sinks each returned table in one streaming pass: a
publisher or transformer output straight to the storage location the
request assigns it, a subscriber output to a parquet under `--output-folder`
for the destination plugin. Emits an outputs manifest at
`--outputs-manifest` with the URI and the footer stats of each written
table (neither, if the function returned `None` for that table — "not
modified this run").

The run's state (`<state-folder>/state.parquet`, drained by
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

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.function.arguments import (
    RunFunctionArguments,
)
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
INFORMATION = (
    "run-function — invoke the decorated function on the prepared inputs "
    "and sink the returned tables to parquet"
)
MANUAL = (
    "run-function --request <PATH> --bundle-folder <PATH> "
    "--inputs-manifest <PATH> --output-folder <PATH> "
    "--outputs-manifest <PATH> --state-folder <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-folder, materializes each "
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


def build_command_parser() -> ArgumentsParser:
    return RunFunctionArguments.parser(
        program="run-function",
        description="Invoke the publisher's user-defined function.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_function", parser=build_command_parser)
def main(arguments: RunFunctionArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
