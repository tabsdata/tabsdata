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
from tabsdatak.conn.mssql import (
    MSSQLDest,
    MSSQLDestConn,
    MSSQLSrc,
    MSSQLSrcConn,
    _impl as impl,
    _plugin as plugin,
)
from tabsdatak.conn.mssql._explorer import MSSQLServerExplorer
from tabsdatak.conn.mssql.error import (
    MssqlErrorCode,
    MSSQLRuntimeException,
)

# noinspection PyProtectedMember
from test_tabsdatak.conn.common.sql._testing import CtxStub

pytestmark = pytest.mark.mssql


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
# _MSSQLSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-MSSQL happy paths live in the integration suite's ``test_plugin_src.py``
# and ``test_plugin_dest.py``.
# ---------------------------------------------------------------------------


def _bare_src_conn() -> MSSQLSrcConn:
    """Minimal MSSQLSrcConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return MSSQLSrcConn(
        uri="mssql://stub.invalid:1433/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def _bare_dest_conn() -> MSSQLDestConn:
    """Minimal MSSQLDestConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return MSSQLDestConn(
        uri="mssql://stub.invalid:1433/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def test_src_plugin_restarts_on_operational_error_with_mssql_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(queries=["SELECT 1"])
    [slot] = plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    with pytest.raises(RestartRead) as ei:
        list(slot)
    assert isinstance(ei.value.cause, MSSQLRuntimeException)
    assert ei.value.cause.api_error is MssqlErrorCode.MSSQL_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_mssql_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(queries=["SELECT 1"])
    [slot] = plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    with pytest.raises(MSSQLRuntimeException) as ei:
        list(slot)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_6
    assert "query_index=0" in str(ei.value)


# ---------------------------------------------------------------------------
# _MSSQLDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_mssql_7_on_tables_slots_mismatch(work_dir):
    dest = MSSQLDest(tables=["users", "orders"])
    src = pl.LazyFrame([{"id": 1}])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(
            _dest_ctx(work_dir),
            _bare_dest_conn(),
            dest,
            [src],
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_7


def test_dest_plugin_wraps_operational_error_as_mssql_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = pl.LazyFrame([{"id": 1}])
    dest = MSSQLDest(tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_mssql_9(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = pl.LazyFrame([{"id": 1}])
    dest = MSSQLDest(tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_9


def test_dest_plugin_leaves_the_table_of_a_none_slot_untouched(monkeypatch, work_dir):
    written: list[str] = []
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(
        saw_writer.PolarsWriteEngine,
        "loader",
        lambda self: lambda **options: written.append(options["table"]),
    )
    dest = MSSQLDest(
        tables=["users", "orders", "events"],
        if_table_exists="replace",
        transactional=False,
    )
    plugin.MSSQLDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_dest_conn(),
        dest,
        [pl.LazyFrame([{"id": 1}]), None, pl.LazyFrame([{"id": 3}])],
    )
    assert written == ["users", "events"]


def test_dest_plugin_writes_nothing_when_every_slot_is_none(monkeypatch, work_dir):
    build = MagicMock()
    monkeypatch.setattr(impl, "build_mssql_engine", build)
    plugin.MSSQLDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_dest_conn(),
        MSSQLDest(tables=["users", "orders"], if_table_exists="replace"),
        [None, None],
    )
    build.assert_not_called()


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_mssql_src_def_wires_plugin_and_explorer():
    assert plugin.MSSQL_SRC.conn is MSSQLSrcConn
    assert plugin.MSSQL_SRC.src is MSSQLSrc
    assert plugin.MSSQL_SRC.plugin is plugin.MSSQLSrcPlugin
    assert plugin.MSSQL_SRC.explorer is MSSQLServerExplorer
    assert plugin.MSSQL_SRC.type_ == "mssql-sql-in"


def test_mssql_dest_def_wires_plugin_and_explorer():
    assert plugin.MSSQL_DEST.conn is MSSQLDestConn
    assert plugin.MSSQL_DEST.dest is MSSQLDest
    assert plugin.MSSQL_DEST.plugin is plugin.MSSQLDestPlugin
    assert plugin.MSSQL_DEST.explorer is MSSQLServerExplorer
    assert plugin.MSSQL_DEST.type_ == "mssql-sql-out"


def test_mssql_src_cardinality_uses_queries_length():
    src = MSSQLSrc(queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.MSSQL_SRC.cardinality(src) == 3


def test_mssql_dest_cardinality_uses_tables_length():
    dest = MSSQLDest(tables=["users", "orders"])
    assert plugin.MSSQL_DEST.cardinality(dest) == 2


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
        "mssql.logging_level": 20,
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
        "mssql.logging_level": 20,
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
# ``MSSQLSrcPlugin.read_in`` actually feeds its result into
# ``MSSQLReadCfg`` rather than the raw Src field.
# ---------------------------------------------------------------------------


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, schemas, drop_metadata):
            captured["initial_values"] = read_cfg.initial_values
            # One slot per query, as ``SqlReadEngine.read`` guarantees.
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    ctx = CtxStub(work_dir, attributes={"last_modified": "from-ctx"})
    src = MSSQLSrc(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.MSSQLSrcPlugin().read_in(ctx, _bare_src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}


def test_read_in_hands_the_engine_the_schemas_of_its_ctx(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, schemas, drop_metadata):
            captured["schemas"] = schemas
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    ctx = CtxStub(work_dir)
    src = MSSQLSrc(queries=["SELECT * FROM t"])
    plugin.MSSQLSrcPlugin().read_in(ctx, _bare_src_conn(), src)
    captured["schemas"].remember(0, pl.Schema({"a": pl.Int64}))
    assert ctx.get_attr("@td.schema.0") == '{"a": "Int64"}'


def test_read_in_hands_the_engine_its_error_codes(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, schemas, drop_metadata):
            captured["faults"] = faults
            captured["drop_metadata"] = drop_metadata
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    conn = _bare_src_conn()
    src = MSSQLSrc(queries=["SELECT * FROM t"])
    plugin.MSSQLSrcPlugin().read_in(CtxStub(work_dir), conn, src)
    assert captured == {
        "faults": SqlReadFaults(
            unreachable=MssqlErrorCode.MSSQL_5,
            failed=MssqlErrorCode.MSSQL_6,
            uri=conn.uri,
        ),
        "drop_metadata": False,
    }


def test_read_in_drops_metadata_when_src_cfg_asks(work_dir, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        def read(self, read_cfg, faults, *, schemas, drop_metadata):
            captured["drop_metadata"] = drop_metadata
            return [[]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    src = MSSQLSrc(
        queries=["SELECT * FROM t"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    plugin.MSSQLSrcPlugin().read_in(CtxStub(work_dir), _bare_src_conn(), src)
    assert captured["drop_metadata"] is True
