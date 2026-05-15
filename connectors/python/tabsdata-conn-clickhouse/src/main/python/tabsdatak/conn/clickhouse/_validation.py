from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation

_UNQUOTED_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise ClickHouseErrorCode.CLICKHOUSE_3.exception()
    if value.startswith("`") and value.endswith("`") and len(value) >= 2:
        body = value[1:-1].replace("``", "")
        if "\x00" in body or "`" in body:
            # noinspection PyProtectedMember
            raise ClickHouseErrorCode.CLICKHOUSE_5.exception(value=value)
        if not body:
            # noinspection PyProtectedMember
            raise ClickHouseErrorCode.CLICKHOUSE_6.exception(value=value)
        return value
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        body = value[1:-1].replace('""', "")
        if '"' in body or "\x00" in body:
            # noinspection PyProtectedMember
            raise ClickHouseErrorCode.CLICKHOUSE_7.exception(value=value)
        if not body:
            # noinspection PyProtectedMember
            raise ClickHouseErrorCode.CLICKHOUSE_8.exception(value=value)
        return value
    if not _UNQUOTED_RE.match(value):
        # noinspection PyProtectedMember
        raise ClickHouseErrorCode.CLICKHOUSE_9.exception(value=value)
    return value


IDENTIFIER_VALIDATOR = AfterValidator(_validate_identifier)


_URI_RE = re.compile(r"^clickhouse(?:\+[a-z0-9]+)?://", re.IGNORECASE)


def _validate_uri(value: str) -> str:
    if not _URI_RE.match(value):
        # noinspection PyProtectedMember
        raise ClickHouseErrorCode.CLICKHOUSE_4.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(_validate_uri)

# --- Table FQN ------------------------------------------------------


def _validate_table_fqn(value: str) -> str:
    try:
        parts = _validation.split_fqn(value, quote="`")
    except ValueError as e:
        # noinspection PyProtectedMember
        raise ClickHouseErrorCode.CLICKHOUSE_9.exception(cause=e, value=value)
    if not 1 <= len(parts) <= 2:
        # noinspection PyProtectedMember
        raise ClickHouseErrorCode.CLICKHOUSE_10.exception(value=value)
    for p in parts:
        _validate_identifier(p)
    return value


TABLE_FQN_VALIDATOR = AfterValidator(_validate_table_fqn)
