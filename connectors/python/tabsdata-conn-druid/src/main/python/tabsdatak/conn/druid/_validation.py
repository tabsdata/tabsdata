from __future__ import annotations

from pydantic import AfterValidator

from tabsdatak.conn.druid.error import DruidErrorCode

# --- Datasource (Druid's per-table identifier) ----------------------
#
# Druid datasources are flat (no schemas / databases), so this is a
# single-token validator rather than an FQN one. The rules are
# druid-specific: 1-128 chars, no leading ``.`` or ``_``, and no
# ``/`` ``\`` or NUL anywhere in the body.

_DATASOURCE_FORBIDDEN = set("/\\\x00")


def _validate_datasource(value: str) -> str:
    if not (1 <= len(value) <= 128):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_3.exception(length=len(value), value=value)
    if value.startswith(".") or value.startswith("_"):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_4.exception(value=value)
    if set(value) & _DATASOURCE_FORBIDDEN:
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_5.exception(value=value)
    return value


DATASOURCE_VALIDATOR = AfterValidator(_validate_datasource)


# --- Broker URL -----------------------------------------------------

# Pure-regex form used as ``value_pattern`` on the ``UrlSpec``'s Secret
# branch.
URL_PATTERN = r"(?i)https?://[^/\s]+(?:/.*)?"
