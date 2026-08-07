#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- Table FQN ------------------------------------------------------

# ClickHouse tables can be referenced as ``table`` or
# ``database.table``. Backticks quote each part; a literal backtick
# inside a quoted part is written as ``\`\``` (the doubled-backtick
# convention).
CLICKHOUSE_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``ClickHouseTableNameSpec`` field. Built once
# from ``CLICKHOUSE_FQN_DEF`` so per-field annotations stay
# declarative.
CLICKHOUSE_TABLE_FQN_VALIDATION = name_validate(
    CLICKHOUSE_FQN_DEF, max_parts=2, syntax="ClickHouse"
)


# --- Identifier ----------------------------------------------------

# Database: ClickHouse identifier shape (alphanumeric + underscore).
IDENTIFIER_PATTERN = r"^(?:[A-Za-z_][A-Za-z0-9_]*)$"

IDENTIFIER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(IDENTIFIER_PATTERN, ClickHouseErrorCode.CLICKHOUSE_11)
)
