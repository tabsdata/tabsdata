#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from datetime import datetime

from croniter import croniter

from tabsdata._utils.datetime import (
    NoTimezoneError,
    datetime_to_microseconds_iso_str,
    normalize_iso_datetime,
)
from tabsdata.exceptions import DecoratorConfigurationError, ErrorCode


class CronTrigger:
    """
    Represents a cron-based trigger for scheduling Tabsdata publishers,
    subscribers and transformers.

    This class defines a trigger based on a cron expression, with optional
    start and end times to constrain its activation period. It validates the
    cron mask and time boundaries to ensure the trigger is well-formed.
    """

    def __init__(
        self,
        mask: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Initializes a `CronTrigger` instance.

        Args:
            mask: A cron expression string with 5 fields (minute, hour,
                  day of month, month, day of week).
            start: An optional timezone-aware `datetime` object indicating the
                   earliest time the trigger can fire.
            end: An optional timezone-aware `datetime` object indicating the
                 latest time the trigger can fire.
        """
        self.mask = mask
        self.end = end
        self.start = start

    @property
    def mask(self) -> str:
        """
        Gets the cron expression mask.

        Returns:
            The cron expression string.
        """
        return self._mask

    @mask.setter
    def mask(self, mask: str):
        """
        Sets and validates the cron expression mask.

        The mask must be a 5-field cron string.

        Args:
            mask: The cron expression to set.

        Raises:
            DecoratorConfigurationError: If the mask is invalid, has the wrong
                                         number of fields, or uses unsupported
                                         predefined definitions (e.g.,
                                         "@daily").
        """
        if not mask or not isinstance(mask, str):
            raise DecoratorConfigurationError(ErrorCode.DCE16, mask)
        mask = mask.strip()
        if mask.startswith("@"):
            raise DecoratorConfigurationError(ErrorCode.DCE19, mask)
        mask_elements = len(mask.split())
        if mask_elements != 5:
            raise DecoratorConfigurationError(ErrorCode.DCE14, mask, mask_elements)
        if not croniter.is_valid(mask):
            raise DecoratorConfigurationError(ErrorCode.DCE15, mask)
        self._mask = mask

    @property
    def start(self) -> str | None:
        """
        Gets the start time of the trigger as a timezone-aware ISO 8601 string
        with 'Z' timezone.

        Returns:
            The start time in ISO format, or `None` if not set.
        """
        return datetime_to_microseconds_iso_str(self._start)

    @start.setter
    def start(self, start: str | datetime | None):
        """
        Sets and validates the start time for the trigger.

        The start time must be a timezone-aware `datetime` object or a valid
        ISO 8601 string with 'Z' timezone.

        Args:
            start: The start time to set.

        Raises:
            DecoratorConfigurationError: If the start time is not
                                         timezone-aware, has an invalid format,
                                         or is later than the end time.
        """
        try:
            self._start = normalize_iso_datetime(start)
        except NoTimezoneError:
            raise DecoratorConfigurationError(ErrorCode.DCE17, start)
        except Exception:
            raise DecoratorConfigurationError(ErrorCode.DCE11, start, type(start))

        end = getattr(self, "_end", None)
        if start is not None and end is not None and self._start >= end:
            raise DecoratorConfigurationError(ErrorCode.DCE13, start, end)

    @property
    def end(self) -> str | None:
        """
        Gets the end time of the trigger as a timezone-aware ISO 8601 string
        with 'Z' timezone.

        Returns:
            The end time in ISO format, or `None` if not set.
        """
        return datetime_to_microseconds_iso_str(self._end)

    @end.setter
    def end(self, end: str | datetime | None):
        """
        Sets and validates the end time for the trigger.

        The end time must be a timezone-aware `datetime` object or a valid
        ISO 8601 string with 'Z' timezone.

        Args:
            end: The end time to set.

        Raises:
            DecoratorConfigurationError: If the end time is not timezone-aware,
                                         has an invalid format, or is earlier
                                         than the start time.
        """
        try:
            self._end = normalize_iso_datetime(end)
        except NoTimezoneError:
            raise DecoratorConfigurationError(ErrorCode.DCE18, end)
        except Exception:
            raise DecoratorConfigurationError(ErrorCode.DCE12, end, type(end))

        start = getattr(self, "_start", None)
        if start is not None and end is not None and start >= self._end:
            raise DecoratorConfigurationError(ErrorCode.DCE13, start, end)

    def _to_dict(self) -> dict:
        """
        Serializes the `CronTrigger` instance to a dictionary.

        Returns:
            A dictionary representation of the trigger.
        """
        return {
            "mask": self.mask,
            "start": self.start if self.start else None,
            "end": self.end if self.end else None,
        }
