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
from tabsdatak.conn.postgres import PostgresSrcConn, PostgresDestConn, PostgresDest, PostgresSrc
# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _impl as impl
# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _plugin as plugin
from tabsdatak.conn.postgres.error import (
    PostgresRuntimeException,
    PostgresErrorCode,
)


pytestmark = pytest.mark.postgres


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
# _PostgresSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-Postgres happy paths live in ``test__plugin_integration.py``.
# ---------------------------------------------------------------------------


def _bare_src_conn() -> PostgresSrcConn:
    """Minimal PostgresSrcConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return PostgresSrcConn(
        uri="postgresql://stub.invalid:5432/db",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def _bare_dest_conn() -> PostgresDestConn:
    """Minimal PostgresDestConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return PostgresDestConn(
        uri="postgresql://stub.invalid:5432/db",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_postgres_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(conn=_bare_src_conn(), queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_postgres_6(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(conn=_bare_src_conn(), queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_6


# ---------------------------------------------------------------------------
# _PostgresDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_postgres_7_on_tables_slots_mismatch(work_dir):
    dest = PostgresDest(conn=_bare_dest_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_7


def test_dest_plugin_wraps_operational_error_as_postgres_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(conn=_bare_dest_conn(), tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_postgres_9(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(conn=_bare_dest_conn(), tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_9


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    src_exp = plugin.PostgresSrcExplorer()
    assert src_exp.metadata(_bare_src_conn()) is None
    assert src_exp.data(_bare_src_conn(), "SELECT 1") is None
    dest_exp = plugin.PostgresDestExplorer()
    assert dest_exp.metadata(_bare_dest_conn()) is None
    assert dest_exp.data(_bare_dest_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_postgres_src_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_SRC.conn is PostgresSrcConn
    assert plugin.POSTGRES_SRC.src is PostgresSrc
    assert plugin.POSTGRES_SRC.plugin is plugin.PostgresSrcPlugin
    assert plugin.POSTGRES_SRC.explorer is plugin.PostgresSrcExplorer
    assert plugin.POSTGRES_SRC.src_type == "postgres"


def test_postgres_dest_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_DEST.conn is PostgresDestConn
    assert plugin.POSTGRES_DEST.dest is PostgresDest
    assert plugin.POSTGRES_DEST.plugin is plugin.PostgresDestPlugin
    assert plugin.POSTGRES_DEST.explorer is plugin.PostgresDestExplorer
    assert plugin.POSTGRES_DEST.dest_type == "postgres"


def test_postgres_src_cardinality_uses_queries_length():
    src = PostgresSrc(conn=_bare_src_conn(), queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.POSTGRES_SRC.cardinality(src) == 3


def test_postgres_dest_cardinality_uses_tables_length():
    dest = PostgresDest(conn=_bare_dest_conn(), tables=["users", "orders"])
    assert plugin.POSTGRES_DEST.cardinality(dest) == 2


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
            "postgres.logging_level": 20,
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
            "postgres.logging_level": 20,
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
