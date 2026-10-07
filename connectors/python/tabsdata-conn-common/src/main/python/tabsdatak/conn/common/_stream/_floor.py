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


def manifest_bounds(
    cfg: Mapping[str, Any] | None,
    *,
    min_key: str,
    max_key: str,
    trigger_key: str,
) -> tuple[int, int | None]:
    """The manifest bounds a config sets: the seconds no manifest may follow
    the previous one within (at least the manifest floor, which an absent or
    ``None`` value means) and, when given, the seconds no stream may go
    without one (at least the first; absent or ``None`` means never). A plain
    trigger wait below the first is refused too; a marked one lowers the
    floor for a test as before."""
    cfg = cfg or {}
    minimum = cfg.get(min_key)
    if minimum is None:
        minimum = MANIFEST_FLOOR_SECS
    if isinstance(minimum, bool) or not isinstance(minimum, int):
        raise ValueError(
            f"The value of {min_key} must be a whole number of seconds; "
            f"it is {minimum!r}"
        )
    if minimum < MANIFEST_FLOOR_SECS:
        raise ValueError(
            f"The value of {min_key} must be at least {MANIFEST_FLOOR_SECS} "
            f"seconds; it is {minimum}"
        )
    maximum = cfg.get(max_key)
    if maximum is not None:
        if isinstance(maximum, bool) or not isinstance(maximum, int):
            raise ValueError(
                f"The value of {max_key} must be a whole number of seconds; "
                f"it is {maximum!r}"
            )
        if maximum < minimum:
            raise ValueError(
                f"The value of {max_key} must be at least {min_key} "
                f"({minimum} seconds); it is {maximum}"
            )
    trigger = cfg.get(trigger_key)
    if isinstance(trigger, int) and not isinstance(trigger, bool):
        if trigger < SECONDS_BASE and trigger < minimum:
            raise ValueError(
                f"The value of {trigger_key} must be at least {min_key} "
                f"({minimum} seconds); it is {trigger}"
            )
    return minimum, maximum


def manifest_bounds_validator(
    min_key: str, max_key: str, trigger_key: str
) -> AfterValidator:
    def validate(cfg: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        manifest_bounds(cfg, min_key=min_key, max_key=max_key, trigger_key=trigger_key)
        return cfg

    return AfterValidator(validate)
