#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.tableframe._engine.constant.column.names import (
    TD_FILE_PATH_SYSTEM_COLUMN_NAME,
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind, VolatileColumns
from test_tabsdatak.tableframe.support.columns import registry

pytestmark = pytest.mark.tableframe


def test_user_column_is_user_kind():
    assert registry().kind_of("a") is ColumnKind.USER


def test_internal_column_is_internal_kind():
    assert registry().kind_of(TD_IDENTIFIER_SYSTEM_COLUMN_NAME) is ColumnKind.INTERNAL


def test_volatile_column_is_internal_kind():
    assert registry().kind_of(str(VolatileColumns.INDEX)) is ColumnKind.INTERNAL


def test_connector_column_is_connector_kind():
    assert registry().kind_of(TD_FILE_PATH_SYSTEM_COLUMN_NAME) is ColumnKind.CONNECTOR


def _names() -> list[str]:
    return ["a", TD_IDENTIFIER_SYSTEM_COLUMN_NAME, TD_FILE_PATH_SYSTEM_COLUMN_NAME]


def test_keep_user_only():
    kept = registry().keep(_names(), with_internal=False, with_connector=False)
    assert kept == ["a"]


def test_keep_user_and_connector():
    kept = registry().keep(_names(), with_internal=False, with_connector=True)
    assert kept == ["a", TD_FILE_PATH_SYSTEM_COLUMN_NAME]


def test_keep_user_and_internal():
    kept = registry().keep(_names(), with_internal=True, with_connector=False)
    assert kept == ["a", TD_IDENTIFIER_SYSTEM_COLUMN_NAME]


def test_keep_all():
    kept = registry().keep(_names(), with_internal=True, with_connector=True)
    assert kept == _names()
