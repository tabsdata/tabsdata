#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import datetime as dt
import re
import time

import pytest

# noinspection PyProtectedMember
from tabsdatak.common._id import decode_id, encode_id, extract_time_from_uuidv7


def test_encode_id_returns_uuidv7_and_26_char_base32hex():
    u, code = encode_id()
    # UUIDv7: version nibble is 7.
    assert u.version == 7
    # 26 chars, base32hex alphabet (0-9, A-V).
    assert len(code) == 26
    assert re.fullmatch(r"[0-9A-V]{26}", code), code


def test_encode_id_produces_distinct_ids_on_successive_calls():
    a, _ = encode_id()
    b, _ = encode_id()
    assert a != b


def test_decode_id_round_trips_encode_id():
    u, code = encode_id()
    u2, ts, when = decode_id(code)
    assert u2 == u
    assert when.tzinfo is not None
    # Roundtripped timestamp matches the UUID's own time field.
    assert ts == int.from_bytes(u.bytes[:6], byteorder="big")


def test_extract_time_from_uuidv7_returns_tz_aware_utc():
    u, _ = encode_id()
    ts, when = extract_time_from_uuidv7(u)
    assert when.tzinfo is dt.timezone.utc
    assert int(when.timestamp() * 1000) == ts


def test_decode_id_handles_short_code_via_padding():
    u, code = encode_id()
    # Shouldn't raise: the helper pads with '=' internally.
    u2, _, _ = decode_id(code)
    assert u2 == u


def test_encode_id_timestamp_is_close_to_wall_clock():
    before = int(time.time() * 1000)
    u, _ = encode_id()
    after = int(time.time() * 1000)
    ts, _ = extract_time_from_uuidv7(u)
    # Allow a few ms either side for clock granularity.
    assert before - 5 <= ts <= after + 5


def test_decode_id_raises_on_invalid_code():
    with pytest.raises(Exception):
        decode_id("not-a-valid-base32hex-code")
