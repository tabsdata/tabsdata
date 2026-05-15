from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.druid.error import DruidErrorCode

_DATASOURCE_FORBIDDEN = set("/\\\x00")


def _validate_datasource(value: str) -> str:
    if not (1 <= len(value) <= 128):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_3.exception(length=len(value), value=value)
    if value.startswith(".") or value.startswith("_"):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_5.exception(value=value)
    if set(value) & _DATASOURCE_FORBIDDEN:
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_6.exception(value=value)
    return value


DATASOURCE_VALIDATOR = AfterValidator(_validate_datasource)


_URL_RE = re.compile(r"^https?://[^/\s]+(/.*)?$", re.IGNORECASE)


def _validate_url(value: str) -> str:
    if not _URL_RE.match(value):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_4.exception(value=value)
    return value


URL_VALIDATOR = AfterValidator(_validate_url)

# Pure-regex equivalent for use as ``value_pattern``.
URL_PATTERN = r"(?i)https?://[^/\s]+(?:/.*)?"
