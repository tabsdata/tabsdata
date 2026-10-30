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
class RunStreamerArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    bundle_folder: Path = option(
        help="Extracted bundle folder (function_def.json + user code).",
    )
    output_folder: Path = option(
        help=(
            "Local scratch folder the StreamSrcPlugin writes parquets into; "
            "ctx.stage() then moves them to <stage>/files/."
        ),
    )
    connection_file: Path = option(help="Path to the connection definition file.")
