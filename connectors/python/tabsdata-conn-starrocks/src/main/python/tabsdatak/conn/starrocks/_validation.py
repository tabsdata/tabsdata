from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI -------------------------------------------------------------

# Pure-regex form used as ``value_pattern`` on Var / Secret subclasses
# and as ``StringConstraints`` on the bare-str branch of ``UriSpec``.
URI_PATTERN = r"(?i)(?:starrocks|mysql)(?:\+[a-z0-9]+)?://.*"


# --- Table FQN ------------------------------------------------------

# StarRocks (MySQL-protocol-compatible) tables can be referenced as
# ``table`` or ``database.table`` (no schemas). Backticks quote each
# part; a literal backtick inside a quoted part is written as ``````
# (the doubled-backtick convention).
_STARROCKS_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``StarRocksTableNameSpec`` field.
STARROCKS_TABLE_FQN_VALIDATOR = name_validate(_STARROCKS_FQN_DEF, max_parts=2)
