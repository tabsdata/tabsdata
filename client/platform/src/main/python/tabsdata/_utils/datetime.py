#
# Copyright 2025 Tabsdata Inc.
#

from datetime import UTC, datetime


def _convert_timestamp_to_string(timestamp: int | None) -> str:
    if not timestamp:
        return str(timestamp)
    return str(
        datetime.fromtimestamp(timestamp / 1e3, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


class NoTimezoneError(Exception):
    """No timezone error."""

    def __init__(self, *args, **kwargs):
        pass


def datetime_to_microseconds_iso_str(value: datetime | None) -> str | None:
    """
    Converts datetime object to microseconds ISO 8601 str.
    """
    return value.isoformat(timespec="microseconds") if value else None


def normalize_iso_datetime(value: str | datetime | None) -> datetime | None:
    """
    Normalizes datetime object to ISO 8601 format.

    Args:
        value (str | datetime): The date to normalize. The date and time can
            be provided as a string in
            [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or as
            a datetime object.
    Returns:
        datetime: The normalized datetime object. If the input value is None, None is
            returned.
    Raises:
        NoTimezoneError: If the input value does not contain timezone information.
        ValueError: If the input value is not a valid ISO 8601 string or a datetime
            object.
        TypeError: If the input value is not a string, datetime object, or None.
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
