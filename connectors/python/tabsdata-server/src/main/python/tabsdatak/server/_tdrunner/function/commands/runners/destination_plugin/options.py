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
class RunDestinationPluginArguments(CommandArguments):
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option(help="Folder holding the extracted bundle.")
    outputs_manifest: Path = option(
        help="Path to the outputs manifest the function step wrote."
    )
    connection_file: Path = option(help="Path to the connection definition file.")
