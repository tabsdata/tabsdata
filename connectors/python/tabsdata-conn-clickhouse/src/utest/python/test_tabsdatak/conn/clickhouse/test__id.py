#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the upload-id generator."""

from __future__ import annotations

import re
import time

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._id import new_upload_id


pytestmark = pytest.mark.clickhouse


def test_returns_26_char_base32hex() -> None:
    uid = new_upload_id()
    assert isinstance(uid, str)
    assert len(uid) == 26
    # base32hex alphabet: 0-9, A-V (upper-case).
    assert re.fullmatch(r"[0-9A-V]{26}", uid)


def test_unique_per_call() -> None:
    ids = {new_upload_id() for _ in range(64)}
    assert len(ids) == 64


def test_lexicographic_order_matches_creation_order() -> None:
    """UUIDv7's leading bits are unix-millis, and base32hex
    preserves byte order -- so the strings sort by creation time.
    Sleep 2 ms between calls to clear the same-millisecond bucket.
    """
    a = new_upload_id()
    time.sleep(0.002)
    b = new_upload_id()
    time.sleep(0.002)
    c = new_upload_id()
    assert a < b < c
