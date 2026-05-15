from __future__ import annotations

import re

from pydantic import AfterValidator

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode

_IDENT_MAX = 64
_UNQUOTED_RE = re.compile(r"^[A-Za-z0-9_$]+$")
_ALL_DIGITS_RE = re.compile(r"^[0-9]+$")


def _validate_identifier(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_3.exception()
    if len(value) > _IDENT_MAX:
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_5.exception(max=_IDENT_MAX, value=value)
    if value.startswith("`") and value.endswith("`") and len(value) >= 2:
        body = value[1:-1].replace("``", "")
        if "\x00" in body or "`" in body:
            # noinspection PyProtectedMember
            raise StarRocksErrorCode.STARROCKS_6.exception(value=value)
        if not body:
            # noinspection PyProtectedMember
            raise StarRocksErrorCode.STARROCKS_7.exception(value=value)
        return value
    if not _UNQUOTED_RE.match(value):
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_8.exception(value=value)
    if _ALL_DIGITS_RE.match(value):
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_9.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(_validate_identifier)


_URI_RE = re.compile(r"^(?:starrocks|mysql)(?:\+[a-z0-9]+)?://", re.IGNORECASE)


def _validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_4.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(_validate_uri)

# --- Table FQN ------------------------------------------------------


def _validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote="`")
    except ValueError as e:
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_10.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        # noinspection PyProtectedMember
        raise StarRocksErrorCode.STARROCKS_11.exception(value=value)
    for p in parts:
        _validate_identifier(p)
    return value


TABLE_FQN_VALIDATOR = AfterValidator(_validate_table_fqn)
