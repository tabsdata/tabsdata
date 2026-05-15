from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode

_IDENT_MAX_BYTES = 128
_ORDINARY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


def validate_identifier(value: str) -> str:
    if not value:
        raise OracleErrorCode.ORACLE_5.exception()
    if len(value.encode("utf-8")) > _IDENT_MAX_BYTES:
        raise OracleErrorCode.ORACLE_12.exception(max=_IDENT_MAX_BYTES, value=value)
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if '"' in body:
            raise OracleErrorCode.ORACLE_13.exception(value=value)
        if not body:
            raise OracleErrorCode.ORACLE_14.exception(value=value)
        return value
    if not _ORDINARY_RE.match(value):
        raise OracleErrorCode.ORACLE_15.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(validate_identifier)


# --- URI -------------------------------------------------------------


_URI_RE = re.compile(r"^oracle(?:\+[a-z0-9_]+)?://", re.IGNORECASE)


def validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        raise OracleErrorCode.ORACLE_6.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(validate_uri)

# Pure-regex equivalent for use as ``value_pattern``.
URI_PATTERN = r"(?i)oracle(?:\+[a-z0-9_]+)?://.*"

# --- Table FQN ------------------------------------------------------


def validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote='"')
    except ValueError as e:
        raise OracleErrorCode.ORACLE_16.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        raise OracleErrorCode.ORACLE_17.exception(value=value)
    for p in parts:
        validate_identifier(p)
    return value


TABLE_FQN_VALIDATOR = AfterValidator(validate_table_fqn)
