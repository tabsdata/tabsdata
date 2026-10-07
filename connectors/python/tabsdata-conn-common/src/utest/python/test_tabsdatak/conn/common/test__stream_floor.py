#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from typing import Annotated, Any

import pytest
from pydantic import TypeAdapter, ValidationError

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import (
    decode_seconds,
    manifest_bounds,
    manifest_bounds_validator,
    seconds_floor_validator,
    unsafe_seconds,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import MANIFEST_FLOOR_SECS, SECONDS_BASE

KEY = "tabsdata.test.trigger.max_secs"


@pytest.mark.parametrize("value", [MANIFEST_FLOOR_SECS, 61, 3_600, SECONDS_BASE - 1])
def test_a_plain_value_from_the_floor_up_is_itself(value):
    assert decode_seconds(KEY, value) == value


@pytest.mark.parametrize("seconds", [0, 1, 5, MANIFEST_FLOOR_SECS - 1])
def test_a_marked_value_is_its_offset_from_the_base(seconds):
    assert decode_seconds(KEY, SECONDS_BASE + seconds) == seconds


def test_a_marked_value_is_warned_of(caplog):
    with caplog.at_level(logging.WARNING):
        decode_seconds(KEY, unsafe_seconds(5))
    assert KEY in caplog.text
    assert "5 second(s)" in caplog.text


def test_a_plain_value_is_not_warned_of(caplog):
    with caplog.at_level(logging.WARNING):
        decode_seconds(KEY, 120)
    assert caplog.text == ""


@pytest.mark.parametrize("value", [-1, 0, 1, MANIFEST_FLOOR_SECS - 1])
def test_a_value_below_the_floor_is_rejected_plainly(value):
    with pytest.raises(ValueError) as raised:
        decode_seconds(KEY, value)
    assert str(raised.value) == (
        f"The value of {KEY} must be at least {MANIFEST_FLOOR_SECS} seconds; "
        f"it is {value}"
    )


@pytest.mark.parametrize(
    "value",
    [
        SECONDS_BASE + MANIFEST_FLOOR_SECS,
        SECONDS_BASE + MANIFEST_FLOOR_SECS + 1,
        2**63 - 1,
    ],
)
def test_a_value_past_the_marked_zone_is_out_of_range(value):
    with pytest.raises(ValueError, match="out of range"):
        decode_seconds(KEY, value)


@pytest.mark.parametrize("value", [60.0, "60", True, None])
def test_a_value_that_is_not_a_whole_number_is_rejected(value):
    with pytest.raises(ValueError, match="whole number of seconds"):
        decode_seconds(KEY, value)


@pytest.mark.parametrize("seconds", [-1, MANIFEST_FLOOR_SECS])
def test_only_seconds_below_the_floor_can_be_marked(seconds):
    with pytest.raises(ValueError):
        unsafe_seconds(seconds)


def test_a_marked_value_round_trips():
    assert decode_seconds(KEY, unsafe_seconds(7)) == 7


Spec = TypeAdapter(
    Annotated[dict[str, Any] | None, seconds_floor_validator(KEY)],
)


@pytest.mark.parametrize(
    "cfg", [None, {}, {"other": 1}, {KEY: 60}, {KEY: unsafe_seconds(1)}]
)
def test_the_validator_accepts_what_decodes(cfg):
    assert Spec.validate_python(cfg) == cfg


@pytest.mark.parametrize("cfg", [{KEY: 30}, {KEY: unsafe_seconds(1) + 60}])
def test_the_validator_rejects_what_does_not_decode(cfg):
    with pytest.raises(ValidationError):
        Spec.validate_python(cfg)


MIN = "tabsdata.test.manifest.min_secs"
MAX = "tabsdata.test.manifest.max_secs"


def bounds(cfg):
    return manifest_bounds(cfg, min_key=MIN, max_key=MAX, trigger_key=KEY)


@pytest.mark.parametrize(
    ("cfg", "expected"),
    [
        (None, (MANIFEST_FLOOR_SECS, None)),
        ({}, (MANIFEST_FLOOR_SECS, None)),
        ({MIN: None}, (MANIFEST_FLOOR_SECS, None)),
        ({MIN: None, MAX: None}, (MANIFEST_FLOOR_SECS, None)),
        ({MIN: 60}, (60, None)),
        ({MIN: 120}, (120, None)),
        ({MIN: 120, MAX: 120}, (120, 120)),
        ({MAX: 3600}, (MANIFEST_FLOOR_SECS, 3600)),
        ({MIN: 90, KEY: 90}, (90, None)),
        ({MIN: 90, KEY: unsafe_seconds(1)}, (90, None)),
    ],
)
def test_the_manifest_bounds_default_to_the_floor_and_no_heartbeat(cfg, expected):
    assert bounds(cfg) == expected


@pytest.mark.parametrize(
    "cfg",
    [
        {MIN: 59},
        {MIN: 0},
        {MIN: "60"},
        {MIN: True},
        {MAX: 59},
        {MIN: 120, MAX: 119},
        {MAX: "3600"},
        {MIN: 90, KEY: 60},
    ],
    ids=lambda cfg: repr(cfg),
)
def test_the_manifest_bounds_refuse_what_breaks_their_order(cfg):
    with pytest.raises(ValueError):
        bounds(cfg)


Bounds = TypeAdapter(
    Annotated[dict[str, Any] | None, manifest_bounds_validator(MIN, MAX, KEY)],
)


def test_the_bounds_validator_accepts_and_refuses_as_the_decoding_does():
    assert Bounds.validate_python({MIN: 60, MAX: 600}) == {MIN: 60, MAX: 600}
    with pytest.raises(ValidationError):
        Bounds.validate_python({MIN: 60, MAX: 30})
