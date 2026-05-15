from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.postgres.error import PostgresErrorCode

# --- Identifier -----------------------------------------------------


_PG_IDENTIFIER_MAX = 63
_UNQUOTED_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def validate_identifier(value: str) -> str:
    if not value:
        raise PostgresErrorCode.POSTGRES_5.exception()
    if len(value.encode("utf-8")) > _PG_IDENTIFIER_MAX:
        raise PostgresErrorCode.POSTGRES_12.exception(
            max=_PG_IDENTIFIER_MAX, value=value
        )
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if "\x00" in body or '"' in body:
            raise PostgresErrorCode.POSTGRES_13.exception(value=value)
        if not body:
            raise PostgresErrorCode.POSTGRES_14.exception(value=value)
        return value
    if not _UNQUOTED_RE.match(value):
        raise PostgresErrorCode.POSTGRES_15.exception(value=value)
    return value


POSTGRES_IDENTIFIER_VALIDATOR = AfterValidator(validate_identifier)


# --- URI -------------------------------------------------------------


_URI_RE = re.compile(
    r"^postgres(?:ql)?(?:\+[a-z0-9]+)?://",
    re.IGNORECASE,
)


def validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        raise PostgresErrorCode.POSTGRES_6.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(validate_uri)

# Pure-regex equivalent (for use as ``value_pattern`` on Env / Secret
# subclasses); same prefix shape as ``_URI_RE`` plus an unrestricted
# tail so ``re.fullmatch`` matches whole URIs.
POSTGRES_URI_PATTERN = r"(?i)postgres(?:ql)?(?:\+[a-z0-9]+)?://.*"


# --- Table FQN ------------------------------------------------------

# Postgres tables can be referenced as ``table`` or ``schema.table``.
# Double-quotes quote each part.


def validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote='"')
    except ValueError as e:
        raise PostgresErrorCode.POSTGRES_16.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        raise PostgresErrorCode.POSTGRES_17.exception(value=value)
    for p in parts:
        validate_identifier(p)
    return value


POSTGRES_TABLE_FQN_VALIDATOR = AfterValidator(validate_table_fqn)
