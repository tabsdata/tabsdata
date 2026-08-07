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

from tabsdatak.conn.common.sql.constants import (
    SQL_META_INITIAL_VALUES,
    SQL_META_QUERY,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import (
    sqlalchemy_reader as saw_reader,
    sqlalchemy_writer as saw_writer,
)
from tabsdatak.conn.db2 import Db2DestConn, Db2SrcConn, Db2Dest, Db2Src

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _plugin as plugin
from tabsdatak.conn.db2.error import (
    Db2RuntimeException,
    Db2ErrorCode,
)
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer

# noinspection PyProtectedMember
from test_tabsdatak.conn.common.sql._testing import CtxStub

pytestmark = pytest.mark.db2


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
# _Db2SrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-Db2 happy paths live in the integration suite's ``test_plugin_src.py``
# and ``test_plugin_dest.py``.
# ---------------------------------------------------------------------------


def _bare_src_conn() -> Db2SrcConn:
    """Minimal Db2SrcConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return Db2SrcConn(
        uri="db2://stub.invalid:50000/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def _bare_dest_conn() -> Db2DestConn:
    """Minimal Db2DestConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return Db2DestConn(
        uri="db2://stub.invalid:50000/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_db2_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is Db2ErrorCode.DB2_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_db2_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is Db2ErrorCode.DB2_6


# ---------------------------------------------------------------------------
# _Db2DestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_db2_7_on_tables_slots_mismatch(work_dir):
    dest = Db2Dest(tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(
            _dest_ctx(work_dir),
            _bare_dest_conn(),
            dest,
            [src],
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_7


def test_dest_plugin_wraps_operational_error_as_db2_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = Db2Dest(tables=["users"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_db2_9(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = Db2Dest(tables=["users"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_9


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_db2_src_def_wires_plugin_and_explorer():
    assert plugin.DB2_SRC.conn is Db2SrcConn
    assert plugin.DB2_SRC.src is Db2Src
    assert plugin.DB2_SRC.plugin is plugin.Db2SrcPlugin
    assert plugin.DB2_SRC.explorer is Db2ConnectionExplorer
    assert plugin.DB2_SRC.type_ == "db2-sql-in"


def test_db2_dest_def_wires_plugin_and_explorer():
    assert plugin.DB2_DEST.conn is Db2DestConn
    assert plugin.DB2_DEST.dest is Db2Dest
    assert plugin.DB2_DEST.plugin is plugin.Db2DestPlugin
    assert plugin.DB2_DEST.explorer is Db2ConnectionExplorer
    assert plugin.DB2_DEST.type_ == "db2-sql-out"


def test_db2_src_cardinality_uses_queries_length():
    src = Db2Src(
        queries=[
            "SELECT 1 FROM SYSIBM.SYSDUMMY1",
            "SELECT 2 FROM SYSIBM.SYSDUMMY1",
            "SELECT 3 FROM SYSIBM.SYSDUMMY1",
        ],
    )
    assert plugin.DB2_SRC.cardinality(src) == 3


def test_db2_dest_cardinality_uses_tables_length():
    dest = Db2Dest(tables=["users", "orders"])
    assert plugin.DB2_DEST.cardinality(dest) == 2


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
            "db2.logging_level": 20,
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
            "db2.logging_level": 20,
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
# ``Db2SrcPlugin.read_in`` actually feeds its result into
# ``Db2ReadCfg`` rather than the raw Src field.
# ---------------------------------------------------------------------------


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, work_dir_arg):
            captured["initial_values"] = read_cfg.initial_values
            # One slot per query, as ``SqlReadEngine.read`` guarantees.
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    ctx = CtxStub(work_dir, scratch={"last_modified": "from-ctx"})
    src = Db2Src(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.Db2SrcPlugin().read_in(ctx, _bare_src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}


def test_read_in_reports_query_and_binds_as_metadata(work_dir, monkeypatch):
    class _FakeEngine:
        def read(self, read_cfg, work_dir_arg):
            return [[work_dir_arg / "slot0_chunk0.parquet"]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    src = Db2Src(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "2026-01-01"},
    )
    [[table]] = plugin.Db2SrcPlugin().read_in(CtxStub(work_dir), _bare_src_conn(), src)
    assert table.metadata == {
        SQL_META_QUERY: "SELECT * FROM t WHERE ts > :last_modified",
        SQL_META_INITIAL_VALUES: '{"last_modified": "2026-01-01"}',
    }
