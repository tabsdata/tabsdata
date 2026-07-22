#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone

import pytest

# noinspection PyProtectedMember
from tabsdatak.common._datetime import (
    NoTimezoneError,
    _convert_timestamp_to_string,
    datetime_to_microseconds_iso_str,
    normalize_iso_datetime,
)


def test_convert_timestamp_to_string_returns_iso_z_for_positive_millis():
    # 1_700_000_000_000 ms = 2023-11-14T22:13:20 UTC
    assert _convert_timestamp_to_string(1_700_000_000_000) == "2023-11-14T22:13:20Z"


def test_convert_timestamp_to_string_returns_str_none_for_falsy():
    assert _convert_timestamp_to_string(None) == "None"
    assert _convert_timestamp_to_string(0) == "0"


def test_datetime_to_microseconds_iso_str_returns_microsecond_iso():
    dt = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
    assert datetime_to_microseconds_iso_str(dt) == "2026-01-02T03:04:05.678901+00:00"


def test_datetime_to_microseconds_iso_str_passes_through_none():
    assert datetime_to_microseconds_iso_str(None) is None


def test_normalize_iso_datetime_passes_through_aware_datetime():
    dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert normalize_iso_datetime(dt) is dt


def test_normalize_iso_datetime_parses_iso_string():
    out = normalize_iso_datetime("2026-01-01T00:00:00+00:00")
    assert out == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_normalize_iso_datetime_returns_none_for_none():
    assert normalize_iso_datetime(None) is None


def test_normalize_iso_datetime_rejects_naive_datetime():
    naive = datetime(2026, 1, 1)
    with pytest.raises(NoTimezoneError):
        normalize_iso_datetime(naive)


def test_normalize_iso_datetime_rejects_naive_iso_string():
    with pytest.raises(NoTimezoneError):
        normalize_iso_datetime("2026-01-01T00:00:00")


def test_normalize_iso_datetime_raises_value_error_on_garbage_string():
    with pytest.raises(ValueError):
        normalize_iso_datetime("not-a-date")
