#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak.conn.common.types import UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import (
    sqlalchemy_reader as saw_reader,
    sqlalchemy_writer as saw_writer,
)
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn, MySQLDest, MySQLSrc

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _plugin as plugin
from tabsdatak.conn.mysql.error import (
    MySQLRuntimeException,
    MysqlErrorCode,
)
from tabsdatak.conn.mysql._explorer import MySQLConnectionExplorer

# noinspection PyProtectedMember
from test_tabsdatak.conn.common.sql._testing import CtxStub

pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# Test context shims for SrcContext / DestContext
# ---------------------------------------------------------------------------


def _src_ctx(work: Path) -> CtxStub:
    return CtxStub(work)


def _dest_ctx(work: Path) -> CtxStub:
    return CtxStub(work)


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch):
    """Retry tests inject failing engines; shrink the deadlines so
    those tests finish in <1s."""
    monkeypatch.setattr(saw_reader, "RETRY_WAIT_S", 0)
    monkeypatch.setattr(saw_reader, "RETRY_TIMEOUT_S", 1)
    monkeypatch.setattr(saw_writer, "RETRY_WAIT_S", 0)
    monkeypatch.setattr(saw_writer, "RETRY_TIMEOUT_S", 1)


# ---------------------------------------------------------------------------
# _MySQLSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-MySQL happy paths live in the integration suite's ``test_plugin_src.py``
# and ``test_plugin_dest.py``.
# ---------------------------------------------------------------------------


def _bare_src_conn() -> MySQLSrcConn:
    """Minimal MySQLSrcConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return MySQLSrcConn(
        uri="mysql://stub.invalid:3306/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def _bare_dest_conn() -> MySQLDestConn:
    """Minimal MySQLDestConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return MySQLDestConn(
        uri="mysql://stub.invalid:3306/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_mysql_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mysql_engine", lambda *a, **kw: fake)
    src = MySQLSrc(queries=["SELECT 1"])
    with pytest.raises(MySQLRuntimeException) as ei:
        plugin.MySQLSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_mysql_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mysql_engine", lambda *a, **kw: fake)
    src = MySQLSrc(queries=["SELECT 1"])
    with pytest.raises(MySQLRuntimeException) as ei:
        plugin.MySQLSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_6


# ---------------------------------------------------------------------------
# _MySQLDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_mysql_7_on_tables_slots_mismatch(work_dir):
    dest = MySQLDest(tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(MySQLRuntimeException) as ei:
        plugin.MySQLDestPlugin().write_out(
            _dest_ctx(work_dir),
            _bare_dest_conn(),
            dest,
            [src],
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_7


def test_dest_plugin_wraps_operational_error_as_mysql_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mysql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MySQLDest(tables=["users"])
    with pytest.raises(MySQLRuntimeException) as ei:
        plugin.MySQLDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_mysql_9(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mysql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MySQLDest(tables=["users"])
    with pytest.raises(MySQLRuntimeException) as ei:
        plugin.MySQLDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_9


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_mysql_src_def_wires_plugin_and_explorer():
    assert plugin.MYSQL_SRC.conn is MySQLSrcConn
    assert plugin.MYSQL_SRC.src is MySQLSrc
    assert plugin.MYSQL_SRC.plugin is plugin.MySQLSrcPlugin
    assert plugin.MYSQL_SRC.explorer is MySQLConnectionExplorer
    assert plugin.MYSQL_SRC.type_ == "mysql-sql-in"


def test_mysql_dest_def_wires_plugin_and_explorer():
    assert plugin.MYSQL_DEST.conn is MySQLDestConn
    assert plugin.MYSQL_DEST.dest is MySQLDest
    assert plugin.MYSQL_DEST.plugin is plugin.MySQLDestPlugin
    assert plugin.MYSQL_DEST.explorer is MySQLConnectionExplorer
    assert plugin.MYSQL_DEST.type_ == "mysql-sql-out"


def test_mysql_src_cardinality_uses_queries_length():
    src = MySQLSrc(queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.MYSQL_SRC.cardinality(src) == 3


def test_mysql_dest_cardinality_uses_tables_length():
    dest = MySQLDest(tables=["users", "orders"])
    assert plugin.MYSQL_DEST.cardinality(dest) == 2


# ---------------------------------------------------------------------------
# _resolve_src_cfg / _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_src_cfg_returns_defaults_when_empty():
    import logging as _logging

    cfg = plugin._resolve_src_cfg(None)
    assert cfg.logging_level == _logging.ERROR
    assert cfg.read_engine is None
    assert cfg.chunk_size is None


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg(
        {
            "mysql.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 500,
        }
    )
    assert cfg.logging_level == 20
    assert cfg.read_engine == "polarsSqlAlchemyRead"
    assert cfg.chunk_size == 500


def test_resolve_src_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_src_cfg({"bogus.key": "value"})
    assert any("Unknown src_cfg key" in r.getMessage() for r in caplog.records)


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg(
        {
            "mysql.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 500,
        }
    )
    assert cfg.logging_level == 20
    assert cfg.write_engine == "polarsSqlAlchemyWrite"
    assert cfg.chunk_size == 500


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# initial_values wiring -- per-bind-name ctx lookup with Src field fallback.
# The resolver itself is exercised in sql-common's
# ``test_initial_values.py``; this smoke test only verifies that
# ``MySQLSrcPlugin.read_in`` actually feeds its result into
# ``MySQLReadCfg`` rather than the raw Src field.
# ---------------------------------------------------------------------------


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, work_dir_arg):
            captured["initial_values"] = read_cfg.initial_values
            return []

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    ctx = CtxStub(work_dir, scratch={"last_modified": "from-ctx"})
    src = MySQLSrc(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.MySQLSrcPlugin().read_in(ctx, _bare_src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}
