#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- URI -------------------------------------------------------------

# Pattern gating ``UriSpec`` via ``StrOrSecretConstraints`` -- enforced on
# a plain-string URI and on a ``Secret``'s hydrated URI value alike.
URI_PATTERN = r"(?i)^(?:oracle(?:\+[a-z0-9_]+)?://.*)$"
# --- Table FQN ------------------------------------------------------

# Oracle tables can be referenced as ``table`` or ``schema.table``.
# Double-quoted identifiers may contain arbitrary chars *except* the
# double-quote itself -- Oracle does not support any escape mechanism
# for embedding ``"`` inside a quoted identifier. ``escape=None``
# accordingly.
_ORACLE_FQN_DEF = FqnDef(separator=".", quote='"', escape=None)

# Validator for the ``OracleTableNameSpec`` field.
ORACLE_TABLE_FQN_VALIDATOR = name_validate(_ORACLE_FQN_DEF, max_parts=2)
