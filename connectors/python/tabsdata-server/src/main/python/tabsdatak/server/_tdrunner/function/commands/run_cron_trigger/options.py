#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    CommandArguments,
    option,
)


@dataclass(frozen=True, kw_only=True)
class RunCronTriggerArguments(CommandArguments):
    trigger_url: str = option(
        help=(
            "Execute endpoint URL. Same env:/val: resolution as the responder "
            "triplet -- the pod profile passes an env:SECRET__TD_TRIGGER_URL "
            "reference injected from the worker's trigger secret."
        ),
    )
    trigger_method: str = option(
        help="HTTP method for the execute endpoint. Same env:/val: resolution.",
    )
    trigger_authorization: str = option(
        help=(
            "Authorization header for the execute endpoint. Same env:/val: "
            "resolution as the responder triplet."
        ),
    )
