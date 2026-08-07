#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

# --- Identifier / Account patterns ----------------------------------

# Each pattern is paired with the ValueConstraints that binds it to an
# error code. The field spec annotates itself with the constraints, so
# one rule gates both arms of a str-or-secret union.
IDENTIFIER_PATTERN = r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"\x00]+(?:""[^"\x00]+)*")$'

IDENTIFIER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(IDENTIFIER_PATTERN, SnowflakeErrorCode.SNOWFLAKE_10)
)
ACCOUNT_PATTERN = r"^(?:[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$"

ACCOUNT_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(ACCOUNT_PATTERN, SnowflakeErrorCode.SNOWFLAKE_9)
)

# --- Table FQN ------------------------------------------------------

# Snowflake tables can be referenced as ``table``, ``schema.table`` or
# ``database.schema.table``. Double-quoted identifiers may contain
# arbitrary chars and use the doubled-quote convention
# (``"weird ""name"``) to embed literal double-quotes.
_SNOWFLAKE_FQN_DEF = FqnDef(separator=".", quote='"', escape='"')

# Validator for the ``SnowflakeTableNameSpec`` field.
SNOWFLAKE_TABLE_FQN_VALIDATION = name_validate(
    _SNOWFLAKE_FQN_DEF, max_parts=3, syntax="Snowflake"
)
