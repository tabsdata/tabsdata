#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from pydantic import AfterValidator

from tabsdatak.conn.common._stream.constants import MANIFEST_FLOOR_SECS, SECONDS_BASE

logger = logging.getLogger(__name__)


def decode_seconds(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"The value of {key} must be a whole number of seconds; it is {value!r}"
        )
    if SECONDS_BASE <= value < SECONDS_BASE + MANIFEST_FLOOR_SECS:
        seconds = value - SECONDS_BASE
        logger.warning(
            f"The value of {key} lowers the manifest floor to {seconds} "
            "second(s); this is not for production use"
        )
        return seconds
    if value < MANIFEST_FLOOR_SECS:
        raise ValueError(
            f"The value of {key} must be at least {MANIFEST_FLOOR_SECS} seconds; "
            f"it is {value}"
        )
    if value >= SECONDS_BASE:
        raise ValueError(f"The value of {key} is out of range: {value}")
    return value


def unsafe_seconds(seconds: int) -> int:
    if not 0 <= seconds < MANIFEST_FLOOR_SECS:
        raise ValueError(
            f"The seconds to mark must be in [0, {MANIFEST_FLOOR_SECS}); "
            f"they are {seconds}"
        )
    return SECONDS_BASE + seconds


def seconds_floor_validator(key: str) -> AfterValidator:
    def validate(cfg: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        if cfg is not None and key in cfg:
            decode_seconds(key, cfg[key])
        return cfg

    return AfterValidator(validate)
