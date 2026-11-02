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
class ConnectionProbeArguments(CommandArguments):
    request: Path = option(help="Path to the probe request JSON.")
    connection_file: Path = option(help="Path to the connection definition file.")
