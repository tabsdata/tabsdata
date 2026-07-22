#
# Copyright 2026 Tabsdata Inc.
#

from datetime import UTC, datetime, timezone

import pytest

from tabsdata._utils.datetime import (
    NoTimezoneError,
    _convert_timestamp_to_string,
    datetime_to_microseconds_iso_str,
    normalize_iso_datetime,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


class TestConvertTimestampToString:
    def test_valid_timestamp(self):
        # 2025-01-01T00:00:00Z in ms
        ts = 1735689600000
        result = _convert_timestamp_to_string(ts)
        assert result == "2025-01-01T00:00:00Z"

    def test_zero_returns_str_zero(self):
        result = _convert_timestamp_to_string(0)
        assert result == "0"

    def test_none_returns_str_none(self):
        result = _convert_timestamp_to_string(None)
        assert result == "None"


class TestDatetimeToMicrosecondsIsoStr:
    def test_valid_datetime(self):
        dt = datetime(2025, 6, 15, 12, 30, 45, 123456, tzinfo=UTC)
        result = datetime_to_microseconds_iso_str(dt)
        assert "2025-06-15" in result
        assert "12:30:45.123456" in result

    def test_none_returns_none(self):
        assert datetime_to_microseconds_iso_str(None) is None


class TestNormalizeIsoDatetime:
    def test_none_returns_none(self):
        assert normalize_iso_datetime(None) is None

    def test_datetime_with_timezone(self):
        dt = datetime(2025, 1, 1, tzinfo=UTC)
        result = normalize_iso_datetime(dt)
        assert result == dt

    def test_string_with_timezone(self):
        result = normalize_iso_datetime("2025-01-01T00:00:00+00:00")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_datetime_without_timezone_raises(self):
        dt = datetime(2025, 1, 1)
        with pytest.raises(NoTimezoneError):
            normalize_iso_datetime(dt)

    def test_string_without_timezone_raises(self):
        with pytest.raises(NoTimezoneError):
            normalize_iso_datetime("2025-01-01T00:00:00")

    def test_invalid_string_raises_valueerror(self):
        with pytest.raises(ValueError):
            normalize_iso_datetime("not-a-date")

    def test_empty_string_returns_none(self):
        assert normalize_iso_datetime("") is None


class TestNoTimezoneError:
    def test_is_exception(self):
        assert issubclass(NoTimezoneError, Exception)

    def test_can_be_raised(self):
        with pytest.raises(NoTimezoneError):
            raise NoTimezoneError("test")
