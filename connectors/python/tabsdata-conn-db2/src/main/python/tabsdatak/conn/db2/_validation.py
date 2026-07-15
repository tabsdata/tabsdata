from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` via ``StrOrSecretConstraints`` -- enforced on
# a plain-string URI and on a ``Secret``'s hydrated URI value alike.
URI_PATTERN = r"(?i)^(?:(?:db2|ibm_db_sa)(?:\+[a-z0-9_]+)?://.*)$"
# --- Table FQN ------------------------------------------------------

# Db2 tables can be referenced as ``table`` or ``schema.table``.
# Double-quoted identifiers may contain arbitrary chars and use the
# doubled-quote convention (``"weird ""name"``) to embed literal
# double-quotes.
_DB2_FQN_DEF = FqnDef(separator=".", quote='"', escape='"')

# Validator for the ``Db2TableNameSpec`` field.
DB2_TABLE_FQN_VALIDATOR = name_validate(_DB2_FQN_DEF, max_parts=2)
