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
class ReportCompletionArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    outputs_manifest: Path = option(
        help="Path to the outputs manifest YAML produced by run-function.",
    )
    response: Path = option(help="Path to write response.yaml.")
