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
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option(help="Folder holding the extracted bundle.")
    inputs_manifest: Path = option(
        help="Path to the inputs manifest the previous step wrote."
    )
    output_folder: Path = option(help="Working folder of the function context.")
    outputs_manifest: Path = option(
        help=(
            "Path where the outputs manifest is written, with the location and "
            "stats of each written table, for report-completion and "
            "run-destination-plugin."
        ),
    )
    state_folder: Path = option(
        help=(
            "Folder holding the run's state.parquet: the function context's state "
            "is read from it, or from the previous run, and drained back to it."
        ),
    )
