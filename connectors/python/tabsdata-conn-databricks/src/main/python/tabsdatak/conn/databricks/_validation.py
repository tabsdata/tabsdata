from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, name_validate

# --- Identifier / warehouse patterns --------------------------------

# Pure-regex forms used as ``value_pattern`` on the Env / Secret
# subclasses for the Databricks identifier-shaped fields
# (``catalog`` / ``schema`` / ``volume`` / ``warehouse`` /
# ``warehouse_id``). They are enforced via
# :class:`~tabsdatak._types.ValOrRef`'s ``re.fullmatch`` on the
# resolved literal / ref payload at ``value()`` time. Length caps
# (255 per part / 100 for warehouse name) are encoded in the regex
# quantifiers so the patterns alone are sufficient.

_UNQUOTED_IDENTIFIER_PART = r"[A-Za-z_][A-Za-z0-9_-]{0,254}"
_BACKTICK_IDENTIFIER_PART = (
    r"`[^`./\s\x00-\x1f\x7f]"
    r"(?:[^`./\x00-\x1f\x7f]{0,251}[^`./\s\x00-\x1f\x7f])?`"
)
_QUALIFIED_PART = rf"(?:{_UNQUOTED_IDENTIFIER_PART}|{_BACKTICK_IDENTIFIER_PART})"

# Identifier: a single per-part identifier (catalog / schema / table /
# volume part), matching the per-part rules above.
IDENTIFIER_PATTERN = _QUALIFIED_PART

# Qualified name: 1-, 2-, or 3-part dotted name; each part follows
# the per-part identifier rules above. Used as ``value_pattern`` for
# the ``volume`` field on ``DatabricksConn`` (which accepts a
# fully-qualified ``catalog.schema.volume`` reference).
QUALIFIED_NAME_PATTERN = rf"{_QUALIFIED_PART}(?:\.{_QUALIFIED_PART}){{0,2}}"

# Warehouse name: 1-100 chars, no leading/trailing whitespace, no
# ASCII control chars.
WAREHOUSE_NAME_PATTERN = (
    r"[^\s\x00-\x1f\x7f](?:[^\x00-\x1f\x7f]{0,98}[^\s\x00-\x1f\x7f])?"
)

# Warehouse id: 16-char lowercase hex.
WAREHOUSE_ID_PATTERN = r"[0-9a-f]{16}"


# --- Table FQN ------------------------------------------------------

# Databricks tables can be referenced as ``table``, ``schema.table``
# or ``catalog.schema.table``. Backticks quote each part; a literal
# backtick inside a quoted part is written as ```` `` ```` (the
# doubled-backtick convention).
_DATABRICKS_FQN_DEF = FqnDef(separator=".", quote="`", escape="`")

# Validator for the ``DatabricksTableNameSpec`` field.
DATABRICKS_TABLE_FQN_VALIDATOR = name_validate(_DATABRICKS_FQN_DEF, max_parts=3)
