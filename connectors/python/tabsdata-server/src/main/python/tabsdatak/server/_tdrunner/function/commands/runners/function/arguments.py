#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    CommandArguments,
    option,
)


@dataclass(frozen=True, kw_only=True)
class RunFunctionArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    bundle_folder: Path = option(help="Extracted bundle folder.")
    inputs_manifest: Path = option(
        help="Path to the YAML inputs manifest produced by stamp-inputs.",
    )
    output_folder: Path = option(
        help="Working folder of the function context.",
    )
    outputs_manifest: Path = option(
        help=(
            "Path where this command writes the YAML outputs manifest with each "
            "output table's URI and stats (neither if the function returned None). "
            "Consumed by run-destination-plugin and report-completion."
        ),
    )
    state_folder: Path = option(
        help=(
            "Local folder holding this run's state, <state-folder>/state.parquet. "
            "When the user function declares a trailing ``ctx: TransactionCtx`` "
            "parameter, the runtime hydrates the ctx scratch from that file "
            "(falling back to system_input[0]'s storage location) before invoking "
            "the function, and drains it back afterwards. The file, when present, "
            "is written to system_output[0]'s storage location."
        ),
    )
