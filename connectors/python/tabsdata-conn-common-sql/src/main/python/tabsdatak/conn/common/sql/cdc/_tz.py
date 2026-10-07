#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

OFFSET = re.compile(r"^([+-])(\d{1,2}):(\d{2})$")
DECIMAL = re.compile(r"^([+-]?)(\d+)\.?\d*$")


def parse_timezone(value: Any) -> tzinfo | None:
    """The zone a database reports, as the databases spell it: an offset
    (``+02:00``, ``-08:00``), an IANA name (``Europe/Madrid``), ``UTC``, or
    Db2's decimal ``hhmmss`` (``20000.``, ``-50000.``, ``0.``). ``SYSTEM`` and
    anything unreadable are ``None``: no zone known."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    if upper in ("UTC", "GMT", "Z"):
        return timezone.utc
    if upper == "SYSTEM":
        return None
    if offset := OFFSET.match(text):
        sign = -1 if offset.group(1) == "-" else 1
        delta = timedelta(hours=int(offset.group(2)), minutes=int(offset.group(3)))
        return bounded(sign * delta)
    if decimal := DECIMAL.match(text):
        sign = -1 if decimal.group(1) == "-" else 1
        digits = int(decimal.group(2))
        delta = timedelta(
            hours=digits // 10000, minutes=digits // 100 % 100, seconds=digits % 100
        )
        return bounded(sign * delta)
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def bounded(delta: timedelta) -> tzinfo | None:
    try:
        return timezone(delta)
    except ValueError:
        return None


def utc_instant(value: datetime, zone: tzinfo | None) -> datetime:
    """The UTC instant a database clock reading stands for: an aware reading
    as it is, a naive one as the server zone's wall clock -- UTC when no zone
    is known, rather than the worker's own clock."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=zone or timezone.utc)
    return value.astimezone(timezone.utc)
