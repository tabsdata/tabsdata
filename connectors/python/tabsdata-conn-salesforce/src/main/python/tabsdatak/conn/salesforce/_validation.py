#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.salesforce.error import SalesforceErrorCode

# --- API name -------------------------------------------------------


_API_NAME_PATTERN = r"[A-Za-z](?:[A-Za-z0-9]|_(?=[A-Za-z0-9]))*(?:__[cr])?"

API_NAME_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(SalesforceErrorCode.SALESFORCE_5, min_len=1, max_len=80),
    _api.Rule.matches(_API_NAME_PATTERN, SalesforceErrorCode.SALESFORCE_8),
)


# --- Instance URL ----------------------------------------------------

# One spelling of the rule: `InstanceUrlSpec` applies it to both arms of its
# str-or-secret union.
INSTANCE_URL_PATTERN = (
    r"https://[A-Za-z0-9][A-Za-z0-9.-]*"
    r"\.(?:salesforce|force|cloudforce|salesforce-setup)\.com/?"
)

INSTANCE_URL_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(INSTANCE_URL_PATTERN, SalesforceErrorCode.SALESFORCE_6)
)


# --- API version -----------------------------------------------------


API_VERSION_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(r"\d{1,3}\.\d{1,2}", SalesforceErrorCode.SALESFORCE_7)
)
