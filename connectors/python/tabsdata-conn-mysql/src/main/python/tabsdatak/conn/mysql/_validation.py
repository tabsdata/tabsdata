from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` via ``StrOrSecretConstraints`` -- enforced on
# a plain-string URI and on a ``Secret``'s hydrated URI value alike.
MSYQL_URI_PATTERN = r"(?i)^(?:(?:mysql|mariadb)(?:\+[a-z0-9]+)?://.*)$"
# --- Table FQN ------------------------------------------------------

# MySQL / MariaDB tables can be referenced as ``table`` or
# ``database.table`` (no schemas). Backticks quote each part; a
# literal backtick inside a quoted part is written as ``\`\``` (the
# doubled-backtick convention).
_MYSQL_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``MySQLTableNameSpec`` field. Built once from
# ``FQN_DEF`` so per-field annotations stay declarative.
MYSQL_TABLE_FQN_VALIDATOR = name_validate(_MYSQL_FQN_DEF, max_parts=2)
