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
from tabsdatak.conn.postgres import (
    PostgresSrcConn,
    PostgresDestConn,
    PostgresDest,
    PostgresSrc,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _plugin as plugin
from tabsdatak.conn.postgres.error import (
    PostgresRuntimeException,
    PostgresErrorCode,
)
from tabsdatak.conn.postgres._explorer import PostgresConnectionExplorer

# noinspection PyProtectedMember
from test_tabsdatak.conn.common.sql._testing import CtxStub

pytestmark = pytest.mark.postgres


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
# _PostgresSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# Real-Postgres happy paths live in the integration suite's ``test_plugin_src.py``
# and ``test_plugin_dest.py``.
# ---------------------------------------------------------------------------


def _bare_src_conn() -> PostgresSrcConn:
    """Minimal PostgresSrcConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return PostgresSrcConn(
        uri="postgresql://stub.invalid:5432/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def _bare_dest_conn() -> PostgresDestConn:
    """Minimal PostgresDestConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret

    return PostgresDestConn(
        uri="postgresql://stub.invalid:5432/db",
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_postgres_5(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_postgres_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), _bare_src_conn(), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_6


# ---------------------------------------------------------------------------
# _PostgresDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_postgres_7_on_tables_slots_mismatch(work_dir):
    dest = PostgresDest(tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir),
            _bare_dest_conn(),
            dest,
            [src],
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_7


def test_dest_plugin_wraps_operational_error_as_postgres_8(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_8
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_postgres_9(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_9


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_postgres_src_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_SRC.conn is PostgresSrcConn
    assert plugin.POSTGRES_SRC.src is PostgresSrc
    assert plugin.POSTGRES_SRC.plugin is plugin.PostgresSrcPlugin
    assert plugin.POSTGRES_SRC.explorer is PostgresConnectionExplorer
    assert plugin.POSTGRES_SRC.type_ == "postgres-sql-in"


def test_postgres_dest_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_DEST.conn is PostgresDestConn
    assert plugin.POSTGRES_DEST.dest is PostgresDest
    assert plugin.POSTGRES_DEST.plugin is plugin.PostgresDestPlugin
    assert plugin.POSTGRES_DEST.explorer is PostgresConnectionExplorer
    assert plugin.POSTGRES_DEST.type_ == "postgres-sql-out"


def test_postgres_src_cardinality_uses_queries_length():
    src = PostgresSrc(queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.POSTGRES_SRC.cardinality(src) == 3


def test_postgres_dest_cardinality_uses_tables_length():
    dest = PostgresDest(tables=["users", "orders"])
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


# ---------------------------------------------------------------------------
# initial_values wiring -- per-bind-name ctx lookup with Src field fallback.
# The resolver itself is exercised in sql-common's
# ``test_initial_values.py``; this smoke test only verifies that
# ``PostgresSrcPlugin.read_in`` actually feeds its result into
# ``PostgresReadCfg`` rather than the raw Src field.
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
    src = PostgresSrc(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.PostgresSrcPlugin().read_in(ctx, _bare_src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}


def test_read_in_reports_query_and_binds_as_metadata(work_dir, monkeypatch):
    class _FakeEngine:
        def read(self, read_cfg, work_dir_arg):
            return [[work_dir_arg / "slot0_chunk0.parquet"]]

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import factory as _factory

    monkeypatch.setattr(_factory, "get_read_engine", lambda *a, **kw: _FakeEngine())

    src = PostgresSrc(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "2026-01-01"},
    )
    [[table]] = plugin.PostgresSrcPlugin().read_in(CtxStub(work_dir), _bare_src_conn(), src)
    assert table.metadata == {
        SQL_META_QUERY: "SELECT * FROM t WHERE ts > :last_modified",
        SQL_META_INITIAL_VALUES: '{"last_modified": "2026-01-01"}',
    }
