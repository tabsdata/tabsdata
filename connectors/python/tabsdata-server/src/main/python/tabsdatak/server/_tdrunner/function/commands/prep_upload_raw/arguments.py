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
class PrepUploadRawArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    inputs_manifest: Path = option(
        help="Path to the enriched inputs manifest written by stamp-inputs.",
    )
    transporter_request: Path = option(
        help="Path where the transporter copy request is written.",
    )
    storage_configuration: Path = option(
        help="Path to the apiserver storage configuration (mount option keys).",
    )
