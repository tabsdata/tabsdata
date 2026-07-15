from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` via ``StrOrSecretConstraints`` -- enforced on
# a plain-string URI and on a ``Secret``'s hydrated URI value alike.
URI_PATTERN = r"(?i)^(?:(?:starrocks|mysql)(?:\+[a-z0-9]+)?://.*)$"
# --- Table FQN ------------------------------------------------------

# StarRocks (MySQL-protocol-compatible) tables can be referenced as
# ``table`` or ``database.table`` (no schemas). Backticks quote each
# part; a literal backtick inside a quoted part is written as ``````
# (the doubled-backtick convention).
_STARROCKS_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``StarRocksTableNameSpec`` field.
STARROCKS_TABLE_FQN_VALIDATOR = name_validate(_STARROCKS_FQN_DEF, max_parts=2)
