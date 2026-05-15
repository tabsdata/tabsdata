from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.mysql.error import MysqlErrorCode

# --- Identifier -----------------------------------------------------


_MYSQL_IDENTIFIER_MAX = 64
_UNQUOTED_RE = re.compile(r"^[A-Za-z0-9_$]+$")
_ALL_DIGITS_RE = re.compile(r"^[0-9]+$")


def validate_identifier(value: str) -> str:
    if not value:
        raise MysqlErrorCode.MYSQL_5.exception()
    if len(value) > _MYSQL_IDENTIFIER_MAX:
        raise MysqlErrorCode.MYSQL_12.exception(max=_MYSQL_IDENTIFIER_MAX, value=value)
    if value.startswith("`") and value.endswith("`") and len(value) >= 2:
        body = value[1:-1].replace("``", "")
        if "\x00" in body or "`" in body:
            raise MysqlErrorCode.MYSQL_13.exception(value=value)
        if not body:
            raise MysqlErrorCode.MYSQL_14.exception(value=value)
        return value
    if not _UNQUOTED_RE.match(value):
        raise MysqlErrorCode.MYSQL_15.exception(value=value)
    if _ALL_DIGITS_RE.match(value):
        raise MysqlErrorCode.MYSQL_16.exception(value=value)
    return value


MYSQL_IDENTIFIER_VALIDATOR = AfterValidator(validate_identifier)


# --- URI -------------------------------------------------------------


_URI_RE = re.compile(r"^(?:mysql|mariadb)(?:\+[a-z0-9]+)?://", re.IGNORECASE)


def validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        raise MysqlErrorCode.MYSQL_6.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(validate_uri)

# Pure-regex equivalent (for use as ``value_pattern`` on Env / Secret
# subclasses); same prefix shape as ``_URI_RE`` plus an unrestricted
# tail so ``re.fullmatch`` matches whole URIs.
MSYQL_URI_PATTERN = r"(?i)(?:mysql|mariadb)(?:\+[a-z0-9]+)?://.*"


# --- Table FQN ------------------------------------------------------

# MySQL/MariaDB tables can be referenced as ``table`` or
# ``database.table`` (no schemas). Backticks quote each part.


def validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote="`")
    except ValueError as e:
        raise MysqlErrorCode.MYSQL_17.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        raise MysqlErrorCode.MYSQL_18.exception(value=value)
    for p in parts:
        validate_identifier(p)
    return value


MYSQL_TABLE_FQN_VALIDATOR = AfterValidator(validate_table_fqn)
