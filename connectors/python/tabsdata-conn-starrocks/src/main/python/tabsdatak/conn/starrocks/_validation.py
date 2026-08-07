#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate
from tabsdatak.conn.starrocks.error import StarRocksErrorCode

# --- Identifier ----------------------------------------------------

# Database: StarRocks identifier shape (alphanumeric + underscore).
IDENTIFIER_PATTERN = r"^(?:[A-Za-z_][A-Za-z0-9_]*)$"

IDENTIFIER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(IDENTIFIER_PATTERN, StarRocksErrorCode.STARROCKS_11)
)

# --- Table FQN ------------------------------------------------------

# StarRocks (MySQL-protocol-compatible) tables can be referenced as
# ``table`` or ``database.table`` (no schemas). Backticks quote each
# part; a literal backtick inside a quoted part is written as ``````
# (the doubled-backtick convention).
_STARROCKS_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``StarRocksTableNameSpec`` field.
STARROCKS_TABLE_FQN_VALIDATION = name_validate(
    _STARROCKS_FQN_DEF, max_parts=2, syntax="StarRocks"
)
