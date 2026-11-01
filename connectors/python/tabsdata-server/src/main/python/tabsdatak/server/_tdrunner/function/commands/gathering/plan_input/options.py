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
class PlanInputArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    inputs_manifest: Path = option(
        help=(
            "Path to write the inputs manifest (one entry per request.input table, "
            "pointing at its storage location)."
        ),
    )
