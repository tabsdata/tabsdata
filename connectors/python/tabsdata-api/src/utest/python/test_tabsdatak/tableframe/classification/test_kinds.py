#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.tableframe._engine.column.meta import name_of
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind, VolatileColumns
from test_tabsdatak.tableframe.support.columns import registry

pytestmark = pytest.mark.tableframe

META_COLUMN = name_of("source")


def test_user_column_is_user_kind():
    assert registry().kind_of("a") is ColumnKind.USER


def test_internal_column_is_internal_kind():
    assert registry().kind_of(TD_IDENTIFIER_SYSTEM_COLUMN_NAME) is ColumnKind.INTERNAL


def test_volatile_column_is_internal_kind():
    assert registry().kind_of(str(VolatileColumns.INDEX)) is ColumnKind.INTERNAL


def test_meta_column_is_meta_kind():
    assert registry().kind_of(META_COLUMN) is ColumnKind.META


def _names() -> list[str]:
    return ["a", TD_IDENTIFIER_SYSTEM_COLUMN_NAME, META_COLUMN]


def test_keep_user_only():
    kept = registry().keep(_names(), with_internal=False, with_meta=False)
    assert kept == ["a"]


def test_keep_user_and_meta():
    kept = registry().keep(_names(), with_internal=False, with_meta=True)
    assert kept == ["a", META_COLUMN]


def test_keep_user_and_internal():
    kept = registry().keep(_names(), with_internal=True, with_meta=False)
    assert kept == ["a", TD_IDENTIFIER_SYSTEM_COLUMN_NAME]


def test_keep_all():
    kept = registry().keep(_names(), with_internal=True, with_meta=True)
    assert kept == _names()


def test_keep_without_user():
    kept = registry().keep(
        _names(), with_internal=True, with_meta=True, with_user=False
    )
    assert kept == [TD_IDENTIFIER_SYSTEM_COLUMN_NAME, META_COLUMN]


def test_keep_nothing():
    kept = registry().keep(
        _names(), with_internal=False, with_meta=False, with_user=False
    )
    assert kept == []
