#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import (
    DialectRenderer,
    SchemaChange,
    TypeWideningMap,
)

TYPE_WIDENING_MAP: TypeWideningMap = {
    "smallint": {"tinyint"},
    "int": {"tinyint", "smallint"},
    "bigint": {"tinyint", "smallint", "int"},
    "double": {"float"},
    "timestamp_ntz": {"date"},
}

_DECIMAL = re.compile(r"^\s*decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.IGNORECASE)


def _base(type_: str) -> str:
    return re.split(r"[\s(<]", type_.strip().lower(), maxsplit=1)[0]


def same_base(old: str, new: str) -> bool:
    return _base(old) == _base(new)


def can_widen(old: str, new: str) -> bool:
    old_decimal, new_decimal = _DECIMAL.match(old), _DECIMAL.match(new)
    if not (old_decimal and new_decimal):
        return False
    old_p, old_s = (int(group) for group in old_decimal.groups())
    new_p, new_s = (int(group) for group in new_decimal.groups())
    return new_s >= old_s and new_p - new_s >= old_p - old_s


class CheckOnlyRenderer(DialectRenderer):
    SUPPORTED_ACTIONS: set = set()

    def column_def(self, name, type_, nullable, default) -> str:
        raise NotImplementedError

    def render_alter(self, table_name: str, change: SchemaChange) -> str | None:
        raise NotImplementedError
