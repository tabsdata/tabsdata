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
class RunStreamSourcePluginArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    bundle_folder: Path = option(
        help="Extracted bundle folder (function_def.json + user code).",
    )
    context_folder: Path = option(
        help=(
            "Local working folder the stream source plugin writes into; "
            "staging a batch uploads its files to the function stage."
        ),
    )
    connection_file: Path = option(help="Path to the connection definition file.")
