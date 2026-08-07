#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate
from tabsdatak.conn.oracle.error import OracleErrorCode

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` -- one spelling of the rule, enforced on a
# plain-string URI and on a ``Secret``'s hydrated URI value alike.
URI_PATTERN = r"(?i)^(?:oracle(?:\+[a-z0-9_]+)?://.*)$"

URI_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(URI_PATTERN, OracleErrorCode.ORACLE_10)
)

# --- Table FQN ------------------------------------------------------

# Oracle tables can be referenced as ``table`` or ``schema.table``.
# Double-quoted identifiers may contain arbitrary chars *except* the
# double-quote itself -- Oracle does not support any escape mechanism
# for embedding ``"`` inside a quoted identifier. ``escape=None``
# accordingly.
_ORACLE_FQN_DEF = FqnDef(separator=".", quote='"', escape=None)

# Validator for the ``OracleTableNameSpec`` field.
ORACLE_TABLE_FQN_VALIDATION = name_validate(
    _ORACLE_FQN_DEF, max_parts=2, syntax="Oracle"
)
