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
class RunManifestTriggerArguments(CommandArguments):
    request: Path = option(help="Path to request.yaml.")
    trigger_url: str = option(
        help=(
            "fn_trigger URL. Same env:/val: resolution as the responder triplet -- "
            "the apiserver typically passes an env:SECRET__... reference via the "
            "pod profile."
        ),
    )
    trigger_method: str = option(
        help="HTTP method for fn_trigger. Same env:/val: resolution.",
    )
    trigger_authorization: str = option(
        help=(
            "Authorization header for fn_trigger. Same env:/val: resolution as the "
            "responder triplet."
        ),
    )
    back_off_seconds: int = option(
        default=30,
        help=(
            "Seconds to wait between full manifests/ scans. During the wait, the "
            "sidecar drains queued deletions from prior iterations. Default 30."
        ),
    )
