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
    request: Path = option(help="Path to request.yaml.")
    bundle_folder: Path = option(help="Extracted bundle folder.")
    outputs_manifest: Path = option(
        help=(
            "Path to the YAML outputs manifest produced by run-function. Entries "
            "are consumed positionally — one parquet per slot in declaration "
            "order. Entries without a `uri` are rejected (subscribers must return "
            "concrete frames for every slot)."
        ),
    )
    connection_file: Path = option(help="Path to the connection definition file.")
