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
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option(help="Folder holding the extracted bundle.")
    output_folder: Path = option(
        help="Folder where the source plugin writes its parquet files."
    )
    inputs_manifest: Path = option(
        help=(
            "Path where the manifest mapping each function input to the parquet "
            "files the source plugin produced is written, for run-function."
        ),
    )
    state_folder: Path = option(
        help=(
            "Folder where the plugin context's state is drained to state.parquet, "
            "for run-function."
        ),
    )
    connection_file: Path = option(help="Path to the connection definition file.")
