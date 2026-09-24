#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from decimal import Decimal
from typing import Any


def orjson_default(obj: Any) -> Any:
    """Encode the values orjson does not handle natively but a CDC row can
    carry. Binary becomes hex, matching how the wal2json and CD-table
    feeds spell a binary literal; a Decimal becomes a float, which is
    lossy beyond 53 bits of mantissa but is what the map layout has always
    emitted."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj).hex()
    raise TypeError(f"Type is not JSON serializable: {type(obj)}")
