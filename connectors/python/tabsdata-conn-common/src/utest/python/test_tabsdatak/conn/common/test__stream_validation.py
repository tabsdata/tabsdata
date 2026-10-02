#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Annotated, Any

import pytest
from pydantic import TypeAdapter, ValidationError

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._validation import (
    check_non_negative_integer,
    check_positive_integer,
    non_negative_integers_validator,
    positive_integers_validator,
)

KEY = "tabsdata.test.buffer.max_rows"
OTHER = "tabsdata.test.trigger.max_bytes"


@pytest.mark.parametrize("value", [1, 60, 10**12])
def test_a_positive_whole_number_is_accepted(value):
    assert check_positive_integer(KEY, value) == value


@pytest.mark.parametrize("value", [0, -1, 1.5, 2.0, "5", True, None])
def test_anything_else_is_rejected(value):
    with pytest.raises(ValueError) as raised:
        check_positive_integer(KEY, value)
    assert str(raised.value) == (
        f"The value of {KEY} must be a positive whole number; it is {value!r}"
    )


Spec = TypeAdapter(
    Annotated[dict[str, Any] | None, positive_integers_validator(KEY, OTHER)],
)


@pytest.mark.parametrize(
    "cfg", [None, {}, {"unchecked": 0}, {KEY: 1}, {KEY: 5, OTHER: 1024}]
)
def test_the_validator_accepts_positive_values_and_ignores_other_keys(cfg):
    assert Spec.validate_python(cfg) == cfg


@pytest.mark.parametrize("cfg", [{KEY: 0}, {OTHER: -1}, {KEY: 1, OTHER: 0.5}])
def test_the_validator_rejects_every_listed_key_that_is_not_positive(cfg):
    with pytest.raises(ValidationError):
        Spec.validate_python(cfg)


@pytest.mark.parametrize("value", [0, 1, 10**12])
def test_a_whole_number_of_zero_or_more_is_accepted(value):
    assert check_non_negative_integer(KEY, value) == value


@pytest.mark.parametrize("value", [-1, 1.5, "0", True, None])
def test_anything_else_is_rejected_as_non_negative(value):
    with pytest.raises(ValueError) as raised:
        check_non_negative_integer(KEY, value)
    assert str(raised.value) == (
        f"The value of {KEY} must be a whole number of zero or more; it is {value!r}"
    )


NonNegative = TypeAdapter(
    Annotated[dict[str, Any] | None, non_negative_integers_validator(KEY)],
)


@pytest.mark.parametrize("cfg", [None, {}, {KEY: 0}, {KEY: 3}])
def test_the_non_negative_validator_accepts_zero_and_up(cfg):
    assert NonNegative.validate_python(cfg) == cfg


def test_the_non_negative_validator_rejects_a_negative_value():
    with pytest.raises(ValidationError):
        NonNegative.validate_python({KEY: -1})
