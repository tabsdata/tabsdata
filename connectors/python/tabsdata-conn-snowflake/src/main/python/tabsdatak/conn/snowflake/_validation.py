from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- Identifier / Account patterns ----------------------------------

# Pure-regex form used as ``value_pattern`` on Env / Secret subclasses
# for the Snowflake identifier-shaped fields (``role`` / ``database`` /
# ``schema`` / ``warehouse`` / ``stage``). Accepts an unquoted
# identifier or a double-quoted identifier honoring the doubled-quote
# escape convention.
IDENTIFIER_PATTERN = r'[A-Za-z_][A-Za-z0-9_$]*|"[^"\x00]+(?:""[^"\x00]+)*"'

# Pure-regex form used as ``value_pattern`` for ``AccountSpec``.
ACCOUNT_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9]"


# --- Table FQN ------------------------------------------------------

# Snowflake tables can be referenced as ``table``, ``schema.table`` or
# ``database.schema.table``. Double-quoted identifiers may contain
# arbitrary chars and use the doubled-quote convention
# (``"weird ""name"``) to embed literal double-quotes.
_SNOWFLAKE_FQN_DEF = FqnDef(separator=".", quote='"', escape='"')

# Validator for the ``SnowflakeTableNameSpec`` field.
SNOWFLAKE_TABLE_FQN_VALIDATOR = name_validate(_SNOWFLAKE_FQN_DEF, max_parts=3)
