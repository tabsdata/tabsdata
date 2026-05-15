from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import (
    SqlWriteCfg,
    SqlWriteEngine,
)


# ---------------------------------------------------------------------------
# SqlWriteCfg
# ---------------------------------------------------------------------------


def test_sql_write_cfg_declares_all_io_fields():
    fields = SqlWriteCfg.__dataclass_fields__
    assert {
        "conn",
        "target_tables",
        "if_table_exists",
        "transactional",
        "cfg",
    } <= set(fields)


def test_sql_write_cfg_default_create_engine_raises_not_implemented():
    class _C(SqlWriteCfg):
        pass

    cfg = _C(
        conn=None,
        target_tables=["t"],
        if_table_exists="append",
        transactional=True,
        cfg=None,
    )
    with pytest.raises(NotImplementedError):
        cfg.create_sql_alchemy_engine()


# ---------------------------------------------------------------------------
# SqlWriteEngine
# ---------------------------------------------------------------------------


def test_sql_write_engine_cannot_be_instantiated_without_override():
    with pytest.raises(TypeError):
        SqlWriteEngine()  # type: ignore[abstract]


def test_sql_write_engine_write_is_abstract():
    assert "write" in SqlWriteEngine.__abstractmethods__
