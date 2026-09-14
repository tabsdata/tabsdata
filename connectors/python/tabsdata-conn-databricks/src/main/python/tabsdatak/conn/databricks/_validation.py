#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate, unfqn
from tabsdatak.conn.databricks.error import DatabricksErrorCode

# --- Identifier / warehouse patterns --------------------------------

# Each pattern is paired with the ValueConstraints that binds it to an
# error code. The field spec annotates itself with the constraints, so
# one rule gates both arms of a str-or-secret union.

_UNQUOTED_IDENTIFIER_PART = r"[A-Za-z_][A-Za-z0-9_-]{0,254}"
_BACKTICK_IDENTIFIER_PART = (
    r"`[^`./\s\x00-\x1f\x7f]"
    r"(?:[^`./\x00-\x1f\x7f]{0,251}[^`./\s\x00-\x1f\x7f])?`"
)
_QUALIFIED_PART = rf"(?:{_UNQUOTED_IDENTIFIER_PART}|{_BACKTICK_IDENTIFIER_PART})"

# Identifier: a single per-part identifier (catalog / schema / table /
# volume part), matching the per-part rules above.
IDENTIFIER_PATTERN = rf"^(?:{_QUALIFIED_PART})$"

IDENTIFIER_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(IDENTIFIER_PATTERN, DatabricksErrorCode.DATABRICKS_14)
)

# Qualified name: 1-, 2-, or 3-part dotted name; each part follows
# the per-part identifier rules above.
QUALIFIED_NAME_PATTERN = rf"^(?:{_QUALIFIED_PART}(?:\.{_QUALIFIED_PART}){{0,2}})$"

QUALIFIED_NAME_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(QUALIFIED_NAME_PATTERN, DatabricksErrorCode.DATABRICKS_17)
)

# Warehouse name: 1-100 chars, no leading/trailing whitespace, no
# ASCII control chars.
WAREHOUSE_NAME_PATTERN = (
    r"^(?:[^\s\x00-\x1f\x7f](?:[^\x00-\x1f\x7f]{0,98}[^\s\x00-\x1f\x7f])?)$"
)

WAREHOUSE_NAME_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(WAREHOUSE_NAME_PATTERN, DatabricksErrorCode.DATABRICKS_15)
)

# Warehouse id: 16-char lowercase hex.
WAREHOUSE_ID_PATTERN = r"^(?:[0-9a-f]{16})$"

WAREHOUSE_ID_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(WAREHOUSE_ID_PATTERN, DatabricksErrorCode.DATABRICKS_16)
)

# --- Table FQN ------------------------------------------------------

# Databricks tables can be referenced as ``table``, ``schema.table``
# or ``catalog.schema.table``. Backticks quote each part; a literal
# backtick inside a quoted part is written as ```` `` ```` (the
# doubled-backtick convention).
_DATABRICKS_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``DatabricksTableNameSpec`` field.
DATABRICKS_TABLE_FQN_VALIDATION = name_validate(
    _DATABRICKS_FQN_DEF, max_parts=3, syntax="Databricks"
)

# --- Merge ----------------------------------------------------------

# A merge column is a single Databricks identifier. ``IDENTIFIER_PATTERN``
# already admits one part only, so a dotted value is rejected rather than
# silently read as a qualified reference. Same shape and same error code as an
# identifier, so ``DatabricksColumnSpec`` annotates itself with that constraint
# under its own name.
COLUMN_VALIDATION = IDENTIFIER_VALIDATION


def merge_column_key(name: str) -> str:
    """The canonical form of a merge column name: unquoted and lower-cased.

    Databricks resolves identifiers case-insensitively, and a backtick quotes a
    special-char name without making it case-sensitive -- so ``order_id``,
    ``ORDER_ID`` and ```OrderId``` all name one column. Folding to a single case
    is therefore the only way to tell the marker column apart from a key
    whichever case either was written in.

    Note this is a canonical *comparison* key, not the name Databricks stores: a
    Delta column keeps the case it was defined with, which a config cannot know.
    That differs from Snowflake, where an unquoted name really is upper-folded on
    the way in and a quoted one really is case-sensitive.
    """
    (part,) = unfqn(name, _DATABRICKS_FQN_DEF)
    return str(part).lower()
