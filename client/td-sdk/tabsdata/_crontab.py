#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from datetime import datetime

from croniter import croniter

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
        mask_elements = len(mask.split())
        if mask_elements != 5:
            raise DecoratorConfigurationError(ErrorCode.DCE14, mask, mask_elements)
        if not croniter.is_valid(mask):
            raise DecoratorConfigurationError(ErrorCode.DCE15, mask)
        self._mask = mask

    @property
    def start(self) -> datetime | None:
        return self._start

    @start.setter
    def start(self, start: datetime | None):
        if start is None or isinstance(start, datetime):
            end = getattr(self, "_end", None)
            if start is not None and end is not None and start >= end:
                raise DecoratorConfigurationError(ErrorCode.DCE13, start, end)
            self._start = start
        else:
            raise DecoratorConfigurationError(ErrorCode.DCE11, start, type(start))

    @property
    def end(self) -> datetime | None:
        return self._end

    @end.setter
    def end(self, end: datetime | None):
        if end is None or isinstance(end, datetime):
            start = getattr(self, "_start", None)
            if start is not None and end is not None and start >= end:
                raise DecoratorConfigurationError(ErrorCode.DCE13, start, end)
            self._end = end
        else:
            raise DecoratorConfigurationError(ErrorCode.DCE12, end, type(end))

    def _to_dict(self) -> dict:
        return {
            "mask": self.mask,
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
        }
