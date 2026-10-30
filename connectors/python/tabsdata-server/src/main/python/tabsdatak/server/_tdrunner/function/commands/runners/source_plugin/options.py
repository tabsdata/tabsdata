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
class RunSourcePluginArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    bundle_folder: Path = option(help="Extracted bundle folder.")
    output_folder: Path = option(
        help="Folder where read_in writes parquet files.",
    )
    inputs_manifest: Path = option(
        help=(
            "Path where this command writes the YAML manifest mapping each "
            "function input parameter to the parquet files produced by the source "
            "plugin. Consumed by the function runner."
        ),
    )
    state_folder: Path = option(
        help=(
            "Local folder where the plugin ctx scratch is drained to "
            "<state-folder>/state.parquet, for run-function to pick up. The previous "
            "run's state is read from system_input[0]'s storage location."
        ),
    )
    connection_file: Path = option(help="Path to the connection definition file.")
