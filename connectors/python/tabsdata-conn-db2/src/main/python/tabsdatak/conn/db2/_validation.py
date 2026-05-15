from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.db2.error import Db2ErrorCode

# --- Identifier (schema / table / column) ----------------------------


_IDENT_MAX_BYTES = 128
_ORDINARY_RE = re.compile(r"^[A-Za-z_@#$][A-Za-z0-9_@#$]*$")


def validate_identifier(value: str) -> str:
    if not value:
        raise Db2ErrorCode.DB2_5.exception()
    if len(value.encode("utf-8")) > _IDENT_MAX_BYTES:
        raise Db2ErrorCode.DB2_12.exception(max=_IDENT_MAX_BYTES, value=value)
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if '"' in body:
            raise Db2ErrorCode.DB2_13.exception(value=value)
        if not body:
            raise Db2ErrorCode.DB2_14.exception(value=value)
        return value
    if not _ORDINARY_RE.match(value):
        raise Db2ErrorCode.DB2_15.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(validate_identifier)


# --- URI -------------------------------------------------------------


_URI_RE = re.compile(r"^(?:db2|ibm_db_sa)(?:\+[a-z0-9_]+)?://", re.IGNORECASE)


def validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        raise Db2ErrorCode.DB2_6.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(validate_uri)

# Pure-regex equivalents for use as ``value_pattern`` on Env / Secret
# subclasses.
URI_PATTERN = r"(?i)(?:db2|ibm_db_sa)(?:\+[a-z0-9_]+)?://.*"
# Identifier (ordinary OR delimited) -- mirrors ``validate_identifier``
# but as a regex; the imperative byte-length cap and embedded-quote
# check stay in the validator and don't ride on the regex.
IDENTIFIER_PATTERN = r'[A-Za-z_@#$][A-Za-z0-9_@#$]*|"[^"\x00]+(?:""[^"\x00]+)*"'


# --- Table FQN ------------------------------------------------------

# Db2 tables can be referenced as ``table`` or ``schema.table``.
# Double quotes delimit each part.


def validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote='"')
    except ValueError as e:
        raise Db2ErrorCode.DB2_16.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        raise Db2ErrorCode.DB2_17.exception(value=value)
    for p in parts:
        validate_identifier(p)
    return value


DB2_TABLE_FQN_VALIDATOR = AfterValidator(validate_table_fqn)
