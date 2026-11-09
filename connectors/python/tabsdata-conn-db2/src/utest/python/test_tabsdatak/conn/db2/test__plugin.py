#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak._handoff import RestartRead

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import (
    sqlalchemy_reader as saw_reader,
    sqlalchemy_writer as saw_writer,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadFaults
from tabsdatak.conn.common.types import UserPassword

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import (
    Db2Dest,
    Db2DestConn,
    Db2Src,
    Db2SrcConn,
    _impl as impl,
    _plugin as plugin,
)
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer
from tabsdatak.conn.db2.error import (
    Db2ErrorCode,
    Db2RuntimeException,
)

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


def test_src_plugin_restarts_on_operational_error_with_db2_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    [slot] = plugin.Db2SrcPlugin().read_in(
        _src_ctx(work_dir), _bare_src_conn(), src
    )
    with pytest.raises(RestartRead) as ei:
        list(slot)
    assert isinstance(ei.value.cause, Db2RuntimeException)
    assert ei.value.cause.api_error is Db2ErrorCode.DB2_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_db2_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    [slot] = plugin.Db2SrcPlugin().read_in(
        _src_ctx(work_dir), _bare_src_conn(), src
    )
    with pytest.raises(Db2RuntimeException) as ei:
        list(slot)
    assert ei.value.api_error is Db2ErrorCode.DB2_6
    assert "query_index=0" in str(ei.value)


# ---------------------------------------------------------------------------
# _Db2DestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_db2_7_on_tables_slots_mismatch(work_dir):
    dest = Db2Dest(tables=["users", "orders"])
    src = pl.LazyFrame([{"id": 1}])
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
    src = pl.LazyFrame([{"id": 1}])
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
    src = pl.LazyFrame([{"id": 1}])
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
    assert cfg.drop_metadata is False


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg({
        "db2.logging_level": 20,
        "tabsdata.sql.read_engine": "polarsSqlAlchemyRead",
        "tabsdata.sql.chunk_size": 500,
        "tabsdata.src_metadata.drop": True,
    })
    assert cfg.logging_level == 20
    assert cfg.read_engine == "polarsSqlAlchemyRead"
    assert cfg.chunk_size == 500
    assert cfg.drop_metadata is True


def test_resolve_src_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    plugin._resolve_src_cfg({"bogus.key": "value"})
    assert any("Unknown src_cfg key" in r.getMessage() for r in caplog.records)


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg({
        "db2.logging_level": 20,
        "tabsdata.sql.write_engine": "polarsSqlAlchemyWrite",
        "tabsdata.sql.chunk_size": 500,
    })
    assert cfg.logging_level == 20
    assert cfg.write_engine == "polarsSqlAlchemyWrite"
    assert cfg.chunk_size == 500


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
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
        def read(self, read_cfg, faults, *, drop_metadata=False):
            captured["initial_values"] = read_cfg.initial_values
            # One slot per query, as ``SqlReadEngine.read`` guarantees.
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    ctx = CtxStub(work_dir, attributes={"last_modified": "from-ctx"})
    src = Db2Src(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.Db2SrcPlugin().read_in(ctx, _bare_src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}


def test_read_in_hands_the_engine_its_error_codes(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, drop_metadata=False):
            captured["faults"] = faults
            captured["drop_metadata"] = drop_metadata
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    conn = _bare_src_conn()
    src = Db2Src(queries=["SELECT * FROM t"])
    plugin.Db2SrcPlugin().read_in(CtxStub(work_dir), conn, src)
    assert captured == {
        "faults": SqlReadFaults(
            unreachable=Db2ErrorCode.DB2_5,
            failed=Db2ErrorCode.DB2_6,
            uri=conn.uri,
        ),
        "drop_metadata": False,
    }


def test_read_in_drops_metadata_when_src_cfg_asks(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, drop_metadata=False):
            captured["drop_metadata"] = drop_metadata
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    src = Db2Src(
        queries=["SELECT * FROM t"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    plugin.Db2SrcPlugin().read_in(CtxStub(work_dir), _bare_src_conn(), src)
    assert captured["drop_metadata"] is True
