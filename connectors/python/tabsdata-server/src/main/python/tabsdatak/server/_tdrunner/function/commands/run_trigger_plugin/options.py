#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    CommandArguments,
)


@dataclass(frozen=True, kw_only=True)
class RunTriggerPluginArguments(CommandArguments):
    pass
