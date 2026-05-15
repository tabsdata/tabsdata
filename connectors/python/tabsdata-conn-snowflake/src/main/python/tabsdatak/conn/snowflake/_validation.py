from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

# --- Identifier -----------------------------------------------------


_IDENT_MAX = 255
_UNQUOTED_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _validate_identifier(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_4.exception()
    if len(value) > _IDENT_MAX:
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_9.exception(max=_IDENT_MAX, value=value)
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if '"' in body:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_10.exception(value=value)
        if not body:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_11.exception(value=value)
        return value
    if not _UNQUOTED_RE.match(value):
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_12.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(_validate_identifier)

# Pure-regex equivalent for use as ``value_pattern`` (unquoted or
# double-quoted Snowflake identifier shape).
IDENTIFIER_PATTERN = r'[A-Za-z_][A-Za-z0-9_$]*|"[^"\x00]+(?:""[^"\x00]+)*"'


# --- Account ---------------------------------------------------------


_ACCOUNT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9]$")


def _validate_account(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_5.exception()
    if not _ACCOUNT_RE.match(value):
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_13.exception(value=value)
    return value


ACCOUNT_VALIDATOR = AfterValidator(_validate_account)

# Pure-regex equivalent for use as ``value_pattern``.
ACCOUNT_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9]"

# --- Table FQN ------------------------------------------------------


def _validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote='"')
    except ValueError as e:
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_14.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 3:
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_15.exception(value=value)
    for p in parts:
        _validate_identifier(p)
    return value


TABLE_FQN_VALIDATOR = AfterValidator(_validate_table_fqn)
