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
class StampInputsArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    inputs_manifest: Path = option(
        help="Path to the YAML inputs manifest written by run-source-plugin.",
    )
    enriched_folder: Path = option(
        help="Folder where stamped parquets are written.",
    )
    enriched_inputs_manifest: Path = option(
        help=(
            "Path where the enriched inputs manifest is written (consumed by "
            "run-function)."
        ),
    )
