#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import (
    FqnDef,
    distinct_names_validate,
    unfqn,
)

# Glue stores database and table names in lowercase, and Athena reads without
# quoting only the names made of lowercase letters, digits and underscores, so
# those are the names accepted.
_NAME_PART = r"[a-z0-9_]{1,255}"

DATABASE_PATTERN = rf"^(?:{_NAME_PART})$"

DATABASE_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(DATABASE_PATTERN, AwsGlueErrorCode.AWSGLUE_3)
)

TABLE_PATTERN = rf"^(?:(?:{_NAME_PART}\.)?{_NAME_PART})$"

TABLE_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(TABLE_PATTERN, AwsGlueErrorCode.AWSGLUE_4)
)

CATALOG_ID_PATTERN = r"^(?:[0-9]{12})$"

CATALOG_ID_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(CATALOG_ID_PATTERN, AwsGlueErrorCode.AWSGLUE_5)
)

_TABLE_FQN_DEF = FqnDef(separator=".")

DISTINCT_TABLES_VALIDATION = distinct_names_validate(_TABLE_FQN_DEF)


def table_parts(name: str) -> tuple[str | None, str]:
    """The database, `None` when the name leaves it out, and the table."""
    parts = [str(part) for part in unfqn(name, _TABLE_FQN_DEF)]
    return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
