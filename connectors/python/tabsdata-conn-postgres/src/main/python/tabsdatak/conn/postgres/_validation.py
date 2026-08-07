#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate
from tabsdatak.conn.postgres.error import PostgresErrorCode

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` -- one spelling of the rule, enforced on a
# plain-string URI and on a ``Secret``'s hydrated URI value alike.
URI_PATTERN = r"(?i)^(?:postgres(?:ql)?(?:\+[a-z0-9]+)?://.*)$"

URI_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(URI_PATTERN, PostgresErrorCode.POSTGRES_19)
)

# --- Table FQN ------------------------------------------------------

# Postgres tables can be referenced as ``table``, ``schema.table`` or
# ``database.schema.table``. Double-quoted identifiers may contain
# arbitrary chars and use the doubled-quote convention
# (``"weird ""name"``) to embed literal double-quotes.
_POSTGRES_FQN_DEF = FqnDef(separator=".", quote='"', escape='"')

# Validator for the ``PostgresTableNameSpec`` field.
POSTGRES_TABLE_FQN_VALIDATION = name_validate(
    _POSTGRES_FQN_DEF, max_parts=3, syntax="Postgres"
)
