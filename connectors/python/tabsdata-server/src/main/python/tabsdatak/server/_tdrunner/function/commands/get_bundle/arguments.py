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
class GetBundleArguments(CommandArguments):
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option(
        help="Folder to extract the bundle into.",
    )
    bundle_archive: Path = option(help="Path to download the bundle archive to.")
