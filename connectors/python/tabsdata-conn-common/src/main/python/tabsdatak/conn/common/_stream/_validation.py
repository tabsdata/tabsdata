#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import AfterValidator


def check_positive_integer(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(
            f"The value of {key} must be a positive whole number; it is {value!r}"
        )
    return value


def check_non_negative_integer(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            f"The value of {key} must be a whole number of zero or more; "
            f"it is {value!r}"
        )
    return value


def non_negative_integers_validator(*keys: str) -> AfterValidator:
    def validate(cfg: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        if cfg is not None:
            for key in keys:
                if key in cfg:
                    check_non_negative_integer(key, cfg[key])
        return cfg

    return AfterValidator(validate)


def positive_integers_validator(*keys: str) -> AfterValidator:
    def validate(cfg: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        if cfg is not None:
            for key in keys:
                if key in cfg:
                    check_positive_integer(key, cfg[key])
        return cfg

    return AfterValidator(validate)
