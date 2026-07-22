#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import timedelta, tzinfo

from dateutil import tz as dateutil_tz

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_server_timezone(value: str | timedelta | None) -> tzinfo | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return dateutil_tz.tzoffset(None, value.total_seconds())
    timezone = str(value).strip()
    if not timezone:
        return None
    tz = dateutil_tz.gettz(timezone)
    if tz is None:
        tz = dateutil_tz.gettz(f"UTC{timezone}")
    if tz is None:
        logger.warning(f"Could not parse server timezone: {timezone}")
    return tz
