#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from tabsdatak.conn.common.sql.cdc._tz import parse_timezone, utc_instant


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("+02:00", timezone(timedelta(hours=2))),
        ("-08:00", timezone(timedelta(hours=-8))),
        ("+05:30", timezone(timedelta(hours=5, minutes=30))),
        ("UTC", timezone.utc),
        ("Etc/UTC", ZoneInfo("Etc/UTC")),
        ("Europe/Madrid", ZoneInfo("Europe/Madrid")),
        (Decimal("20000"), timezone(timedelta(hours=2))),
        ("20000.", timezone(timedelta(hours=2))),
        ("-50000.", timezone(timedelta(hours=-5))),
        (Decimal("0"), timezone.utc),
        ("0.", timezone.utc),
        ("53000", timezone(timedelta(hours=5, minutes=30))),
    ],
    ids=lambda value: repr(value),
)
def test_a_zone_is_read_as_each_database_spells_it(value, expected):
    assert parse_timezone(value) == expected


@pytest.mark.parametrize(
    "value",
    [None, "", "SYSTEM", "nowhere/at-all", "+25:00", "-990000", "abc"],
    ids=lambda value: repr(value),
)
def test_an_unknown_or_impossible_zone_is_none(value):
    assert parse_timezone(value) is None


def test_a_naive_reading_is_the_server_clock():
    zone = timezone(timedelta(hours=-5))
    assert utc_instant(datetime(2026, 1, 1, 12, 0), zone) == datetime(
        2026, 1, 1, 17, 0, tzinfo=timezone.utc
    )


def test_an_aware_reading_keeps_its_instant():
    zone = timezone(timedelta(hours=-5))
    reading = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    assert utc_instant(reading, zone) == datetime(
        2026, 1, 1, 10, 0, tzinfo=timezone.utc
    )


def test_without_a_zone_a_naive_reading_is_taken_as_utc():
    assert utc_instant(datetime(2026, 1, 1, 12, 0), None) == datetime(
        2026, 1, 1, 12, 0, tzinfo=timezone.utc
    )
