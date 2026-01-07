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


class Crontab:
    def __init__(
        self,
        mask: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        self.mask = mask
        self.end = end
        self.start = start

    @property
    def mask(self) -> str:
        return self._mask

    @mask.setter
    def mask(self, mask: str):
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
        return datetime_to_microseconds_iso_str(self._start)

    @start.setter
    def start(self, start: str | datetime | None):
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
        return datetime_to_microseconds_iso_str(self._end)

    @end.setter
    def end(self, end: str | datetime | None):
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
        return {
            "mask": self.mask,
            "start": self.start if self.start else None,
            "end": self.end if self.end else None,
        }
