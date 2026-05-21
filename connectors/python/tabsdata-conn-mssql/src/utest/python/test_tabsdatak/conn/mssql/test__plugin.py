from __future__ import annotations

from dataclasses import dataclass
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
from tabsdatak.conn.mssql import MSSQLConn, MSSQLDest, MSSQLSrc
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _impl as impl
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _plugin as plugin
from tabsdatak.conn.mssql.error import (
    MSSQLRuntimeException,
    MssqlErrorCode,
)


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# Test context shims for SrcContext / DestContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    @property
    def work_dir(self) -> Path:
        return self._work


def _src_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


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
# _MSSQLSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-MSSQL happy paths live in ``test__plugin_integration.py``.
# ---------------------------------------------------------------------------


def _bare_conn() -> MSSQLConn:
    """Minimal MSSQLConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return MSSQLConn(
        uri="mssql://stub.invalid:1433/db",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_mssql_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_mssql_6(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_6


# ---------------------------------------------------------------------------
# _MSSQLDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_mssql_7_on_tables_slots_mismatch(work_dir):
    dest = MSSQLDest(conn=_bare_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_7


def test_dest_plugin_wraps_operational_error_as_mssql_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MSSQLDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is MssqlErrorCode.MSSQL_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_mssql_9(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_mssql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MSSQLDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is MssqlErrorCode.MSSQL_9


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.MSSQLExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_mssql_src_def_wires_plugin_and_explorer():
    assert plugin.MSSQL_SRC.conn is MSSQLConn
    assert plugin.MSSQL_SRC.src is MSSQLSrc
    assert plugin.MSSQL_SRC.plugin is plugin.MSSQLSrcPlugin
    assert plugin.MSSQL_SRC.explorer is plugin.MSSQLExplorer
    assert plugin.MSSQL_SRC.src_type == "mssql"


def test_mssql_dest_def_wires_plugin_and_explorer():
    assert plugin.MSSQL_DEST.conn is MSSQLConn
    assert plugin.MSSQL_DEST.dest is MSSQLDest
    assert plugin.MSSQL_DEST.plugin is plugin.MSSQLDestPlugin
    assert plugin.MSSQL_DEST.explorer is plugin.MSSQLExplorer
    assert plugin.MSSQL_DEST.dest_type == "mssql"


def test_mssql_src_cardinality_uses_queries_length():
    src = MSSQLSrc(conn=_bare_conn(), queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.MSSQL_SRC.cardinality(src) == 3


def test_mssql_dest_cardinality_uses_tables_length():
    dest = MSSQLDest(conn=_bare_conn(), tables=["users", "orders"])
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


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg(
        {
            "mssql.logging_level": 20,
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
            "mssql.logging_level": 20,
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
