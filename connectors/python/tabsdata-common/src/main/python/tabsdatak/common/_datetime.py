#
# Copyright 2026 Tabsdata Inc.
#

"""Small datetime helpers used across tabsdatak."""

from __future__ import annotations

from datetime import UTC, datetime


def _convert_timestamp_to_string(timestamp: int | None) -> str:
    if not timestamp:
        return str(timestamp)
    return str(
        datetime.fromtimestamp(timestamp / 1e3, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


class NoTimezoneError(Exception):
    """The input value lacks timezone information."""

    def __init__(self, *args, **kwargs):
        pass


def datetime_to_microseconds_iso_str(value: datetime | None) -> str | None:
    """Convert a ``datetime`` to a microseconds-precision ISO 8601
    string; returns ``None`` when ``value`` is ``None``."""
    return value.isoformat(timespec="microseconds") if value else None


def normalize_iso_datetime(value: str | datetime | None) -> datetime | None:
    """Normalize a value to a tz-aware ``datetime``.

    Args:
        value: ISO 8601 string, ``datetime``, or ``None``.

    Returns:
        Parsed ``datetime`` (or ``None`` if input was ``None``).

    Raises:
        NoTimezoneError: the input lacks timezone information.
        ValueError: invalid ISO 8601 string.
        TypeError: input is not str / datetime / None.
    """
    if value:
        if isinstance(value, datetime):
            processed_value = value
        else:
            processed_value = datetime.fromisoformat(value)

        if processed_value.tzinfo is None:
            raise NoTimezoneError(value)
    else:
        processed_value = None

    return processed_value
