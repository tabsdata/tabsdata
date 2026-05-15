from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.mssql.error import MssqlErrorCode

# --- Identifier -----------------------------------------------------


_IDENT_MAX = 128
# Regular identifiers: first letter/underscore/@/#; subsequent letters,
# digits, underscores, @, #, $. ``\w`` covers Unicode letters/digits.
_REGULAR_RE = re.compile(r"^[^\W\d][\w@#$]*$", re.UNICODE)


def validate_identifier(value: str) -> str:
    if not value:
        raise MssqlErrorCode.MSSQL_5.exception()
    if len(value) > _IDENT_MAX:
        raise MssqlErrorCode.MSSQL_11.exception(max=_IDENT_MAX, value=value)
    if value.startswith("[") and value.endswith("]") and len(value) >= 2:
        body = value[1:-1].replace("]]", "")
        if "]" in body:
            raise MssqlErrorCode.MSSQL_12.exception(value=value)
        if not body:
            raise MssqlErrorCode.MSSQL_13.exception(value=value)
        return value
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if '"' in body:
            raise MssqlErrorCode.MSSQL_14.exception(value=value)
        if not body:
            raise MssqlErrorCode.MSSQL_15.exception(value=value)
        return value
    if value[0] in "@#" and len(value) > 1 and _REGULAR_RE.match(value[1:]):
        return value
    if value[0] == "_" and (len(value) == 1 or _REGULAR_RE.match(value[1:])):
        return value
    if not _REGULAR_RE.match(value):
        raise MssqlErrorCode.MSSQL_16.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(validate_identifier)


# --- URI / ODBC connection string -----------------------------------
#
# ``MSSQLConn.uri`` accepts two shapes, distinguished by the presence
# of ``://``:
#
# * URI mode  -- ``mssql[+driver]://host[:port][/db]`` (SQLAlchemy
#   URI form). ``+pyodbc`` is the implicit default driver.
# * ODBC mode -- a raw ODBC connection string
#   ``DRIVER={...};SERVER=...;DATABASE=...``. The adapter wraps it
#   as ``mssql+pyodbc:///?odbc_connect=<urlencoded>``.


_URI_RE = re.compile(r"^mssql(?:\+[a-z0-9]+)?://", re.IGNORECASE)
_ODBC_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 ]*=")


def validate_uri(value: str) -> str:
    if _URI_RE.match(value):
        return value
    if _ODBC_RE.match(value):
        return value
    raise MssqlErrorCode.MSSQL_17.exception(value=value)


URI_VALIDATOR = AfterValidator(validate_uri)

# Pure-regex equivalent (for use as ``value_pattern`` on Env / Secret
# subclasses). Matches either form so ``re.fullmatch`` accepts the
# literal payload at ``value()`` time.
MSSQL_URI_PATTERN = r"(?i)(?:mssql(?:\+[a-z0-9]+)?://.*|[A-Za-z][A-Za-z0-9 ]*=.+)"


# --- Table FQN ------------------------------------------------------

# MSSQL tables can be 1-, 2-, or 3-part: ``table``, ``schema.table``
# or ``database.schema.table``. (Linked-server prefixes -- 4-part
# ``server.database.schema.table`` -- are not supported.) Bracketed
# parts are split on '.' boundaries that lie outside ``[...]``.


def validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn_brackets(value)
    except ValueError as e:
        raise MssqlErrorCode.MSSQL_18.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 3:
        raise MssqlErrorCode.MSSQL_19.exception(value=value)
    for p in parts:
        validate_identifier(p)
    return value


TABLE_FQN_VALIDATOR = AfterValidator(validate_table_fqn)
