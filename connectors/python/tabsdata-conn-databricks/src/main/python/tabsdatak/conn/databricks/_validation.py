from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.databricks.error import DatabricksErrorCode

# --- Identifier (catalog / schema / table / volume) -----------------

# Per-part identifier regex shapes (mirrored from the imperative
# validators below). Bound as ``value_pattern`` on the per-field
# ``Env`` / ``Secret`` subclasses in :mod:`tabsdatak.conn.databricks`,
# enforced via :class:`~tabsdatak._types.ValOrRef`'s ``re.fullmatch``
# at ``value()`` time on the resolved literal / ref payload. Length
# caps (255 per part / 100 for warehouse name) are encoded in the
# regex quantifiers so the patterns alone are sufficient.
_UNQUOTED_IDENTIFIER_PART = r"[A-Za-z_][A-Za-z0-9_]{0,254}"
_BACKTICK_IDENTIFIER_PART = (
    r"`[^`./\s\x00-\x1f\x7f]"
    r"(?:[^`./\x00-\x1f\x7f]{0,251}[^`./\s\x00-\x1f\x7f])?`"
)
_QUALIFIED_PART = rf"(?:{_UNQUOTED_IDENTIFIER_PART}|{_BACKTICK_IDENTIFIER_PART})"

# Identifier: a single per-part identifier (catalog / schema / table /
# volume part), matching the per-part rules above.
IDENTIFIER_PATTERN = _QUALIFIED_PART

# Qualified name: 1-, 2-, or 3-part dotted name; each part follows
# the per-part identifier rules above.
QUALIFIED_NAME_PATTERN = rf"{_QUALIFIED_PART}(?:\.{_QUALIFIED_PART}){{0,2}}"

# Warehouse name: 1-100 chars, no leading/trailing whitespace, no
# ASCII control chars.
WAREHOUSE_NAME_PATTERN = (
    r"[^\s\x00-\x1f\x7f](?:[^\x00-\x1f\x7f]{0,98}[^\s\x00-\x1f\x7f])?"
)

# Warehouse id: 16-char lowercase hex.
WAREHOUSE_ID_PATTERN = r"[0-9a-f]{16}"


_UNQUOTED_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Backtick-quoted body: any unicode except backtick, dot, slash, or
# ASCII control char. Whitespace allowed in the middle but not at the
# ends.
_BACKTICK_BODY_RE = re.compile(r"^`(?!\s)(?:[^`./\x00-\x1f\x7f])*(?<!\s)`$")

_DBX_IDENTIFIER_MAX = 255


def _validate_identifier(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_5.exception()
    if len(value) > _DBX_IDENTIFIER_MAX:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_9.exception(
            max=_DBX_IDENTIFIER_MAX, value=value
        )
    if value.startswith("`") and value.endswith("`"):
        if not _BACKTICK_BODY_RE.match(value):
            # noinspection PyProtectedMember
            raise DatabricksErrorCode.DATABRICKS_10.exception(value=value)
        return value
    if not _UNQUOTED_IDENTIFIER_RE.match(value):
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_11.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(_validate_identifier)


# --- Qualified name (1-, 2-, or 3-part) -----------------------------


def _split_qualified(value: str) -> list[str]:
    atoms: list[str] = []
    buf: list[str] = []
    in_quote = False
    for ch in value:
        if ch == "`":
            in_quote = not in_quote
            buf.append(ch)
        elif ch == "." and not in_quote:
            atoms.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    atoms.append("".join(buf))
    if in_quote:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_6.exception(value=value)
    return atoms


def _validate_qualified_name(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_12.exception()
    parts = _split_qualified(value)
    if not (1 <= len(parts) <= 3):
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_13.exception(value=value)
    for p in parts:
        _validate_identifier(p)
    return value


QUALIFIED_NAME_VALIDATOR = AfterValidator(_validate_qualified_name)


# --- Warehouse name -------------------------------------------------


_WAREHOUSE_NAME_MAX = 100
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _validate_warehouse_name(value: str) -> str:
    if not (1 <= len(value) <= _WAREHOUSE_NAME_MAX):
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_7.exception(
            max=_WAREHOUSE_NAME_MAX, length=len(value), value=value
        )
    if value != value.strip():
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_14.exception(value=value)
    if _CONTROL_RE.search(value):
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_15.exception(value=value)
    return value


WAREHOUSE_NAME_VALIDATOR = AfterValidator(_validate_warehouse_name)


# --- Warehouse id ---------------------------------------------------


_WAREHOUSE_ID_RE = re.compile(r"^[0-9a-f]{16}$")


def _validate_warehouse_id(value: str) -> str:
    if not _WAREHOUSE_ID_RE.match(value):
        # noinspection PyProtectedMember
        raise DatabricksErrorCode.DATABRICKS_8.exception(value=value)
    return value


WAREHOUSE_ID_VALIDATOR = AfterValidator(_validate_warehouse_id)
