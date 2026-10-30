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
class RunTransporterArguments(CommandArguments):
    request: Path = option(help="Path to the transporter request YAML.")
    report: Path = option(
        help="Path to write the transporter execution report. Must not already exist.",
    )
