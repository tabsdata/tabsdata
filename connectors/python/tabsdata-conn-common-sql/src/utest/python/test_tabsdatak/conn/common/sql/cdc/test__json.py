#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from decimal import Decimal

import orjson
import pytest

from tabsdatak.conn.common.sql.cdc._json import orjson_default


def _dumps(value) -> str:
    return orjson.dumps({"c": value}, default=orjson_default).decode()


def test_decimal_is_encoded_as_a_number():
    assert _dumps(Decimal("1.50")) == '{"c":1.5}'


@pytest.mark.parametrize(
    "value", [b"\x00\x01", bytearray(b"\x00\x01"), memoryview(b"\x00\x01")]
)
def test_binary_is_encoded_as_hex(value):
    assert _dumps(value) == '{"c":"0001"}'


def test_unsupported_type_still_raises():
    with pytest.raises(TypeError):
        _dumps(object())
