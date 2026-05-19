from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.salesforce.error import SalesforceErrorCode

# --- API name -------------------------------------------------------


_API_NAME_RE = re.compile(
    r"[A-Za-z](?:[A-Za-z0-9]|_(?=[A-Za-z0-9]))*"
    r"(?:__[cr])?"
)


def _validate_api_name(value: str) -> str:
    if not (1 <= len(value) <= 80):
        # noinspection PyProtectedMember
        raise SalesforceErrorCode.SALESFORCE_5.exception(length=len(value), value=value)
    if not _API_NAME_RE.fullmatch(value):
        # noinspection PyProtectedMember
        raise SalesforceErrorCode.SALESFORCE_8.exception(value=value)
    return value


API_NAME_VALIDATOR = AfterValidator(_validate_api_name)


# --- Instance URL ----------------------------------------------------


_INSTANCE_URL_RE = re.compile(
    r"https://[A-Za-z0-9][A-Za-z0-9.-]*"
    r"\.(?:salesforce|force|cloudforce|salesforce-setup)\.com/?"
)


def _validate_instance_url(value: str) -> str:
    if not _INSTANCE_URL_RE.fullmatch(value):
        # noinspection PyProtectedMember
        raise SalesforceErrorCode.SALESFORCE_6.exception(value=value)
    return value


INSTANCE_URL_VALIDATOR = AfterValidator(_validate_instance_url)

# Pure-regex equivalent for use as ``value_pattern``.
INSTANCE_URL_PATTERN = (
    r"https://[A-Za-z0-9][A-Za-z0-9.-]*"
    r"\.(?:salesforce|force|cloudforce|salesforce-setup)\.com/?"
)


# --- API version -----------------------------------------------------


_API_VERSION_RE = re.compile(r"\d{1,3}\.\d{1,2}")


def _validate_api_version(value: str) -> str:
    if not _API_VERSION_RE.fullmatch(value):
        # noinspection PyProtectedMember
        raise SalesforceErrorCode.SALESFORCE_7.exception(value=value)
    return value


API_VERSION_VALIDATOR = AfterValidator(_validate_api_version)
