#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.druid.error import DruidErrorCode

# --- Datasource (Druid's per-table identifier) ----------------------
#
# Druid datasources are flat (no schemas / databases), so these are
# single-token rules rather than FQN ones, and druid-specific: 1-128
# chars, no leading ``.`` or ``_``, and no ``/`` ``\`` or NUL anywhere in
# the body. Each rule reports its own code.

DATASOURCE_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(DruidErrorCode.DRUID_3, min_len=1, max_len=128),
    _api.Rule.check(
        lambda value: not value.startswith((".", "_")), DruidErrorCode.DRUID_4
    ),
    _api.Rule.forbids("/\\\x00", DruidErrorCode.DRUID_5),
)


# --- Broker URL -----------------------------------------------------

# One spelling of the rule: `UrlSpec` applies it to both arms of its
# str-or-secret union.
URL_PATTERN = r"(?i)https?://[^/\s]+(?:/.*)?"

URL_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(URL_PATTERN, DruidErrorCode.DRUID_15)
)
