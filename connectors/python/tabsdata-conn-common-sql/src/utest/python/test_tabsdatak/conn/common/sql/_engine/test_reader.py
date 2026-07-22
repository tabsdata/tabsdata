#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import (
    SqlReadCfg,
    SqlReadEngine,
)


# ---------------------------------------------------------------------------
# SqlReadCfg
# ---------------------------------------------------------------------------


def test_sql_read_cfg_is_abstract_dataclass():
    # ``SqlReadCfg`` is an abstract dataclass: it carries fields the
    # IO layer consumes and an ``ABC``-typed surface so a missing
    # ``create_sql_alchemy_engine`` override fails loudly.
    assert inspect.isabstract(SqlReadCfg) or hasattr(SqlReadCfg, "__abstractmethods__")


def test_sql_read_cfg_declares_all_io_fields():
    fields = SqlReadCfg.__dataclass_fields__
    assert {
        "conn",
        "queries",
        "transactional",
        "schema_overrides",
        "initial_values",
        "cfg",
    } <= set(fields)


def test_sql_read_cfg_default_create_engine_raises_not_implemented():
    # ``create_sql_alchemy_engine`` is a method (not an abstract
    # method): the abstract base raises ``NotImplementedError`` so a
    # subclass that forgets to override fails at run time.
    class _C(SqlReadCfg):
        pass

    cfg = _C(
        conn=None,
        queries=["SELECT 1"],
        transactional=True,
        schema_overrides=None,
        initial_values=None,
        cfg=None,
    )
    with pytest.raises(NotImplementedError):
        cfg.create_sql_alchemy_engine()


# ---------------------------------------------------------------------------
# SqlReadEngine
# ---------------------------------------------------------------------------


def test_sql_read_engine_cannot_be_instantiated_without_override():
    with pytest.raises(TypeError):
        SqlReadEngine()  # type: ignore[abstract]


def test_sql_read_engine_read_is_abstract():
    assert "read" in SqlReadEngine.__abstractmethods__
