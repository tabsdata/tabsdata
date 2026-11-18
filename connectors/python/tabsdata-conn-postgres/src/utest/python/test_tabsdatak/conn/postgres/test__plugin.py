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
from tabsdatak.conn.postgres import (
    PostgresDest,
    PostgresDestConn,
    PostgresSrc,
    PostgresSrcConn,
    _impl as impl,
    _plugin as plugin,
)
from tabsdatak.conn.postgres._explorer import PostgresConnectionExplorer
from tabsdatak.conn.postgres.error import (
    PostgresErrorCode,
    PostgresRuntimeException,
)

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


def test_src_plugin_restarts_on_operational_error_with_postgres_5(
    monkeypatch, work_dir
):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(queries=["SELECT 1"])
    [slot] = plugin.PostgresSrcPlugin().read_in(
        _src_ctx(work_dir), _bare_src_conn(), src
    )
    with pytest.raises(RestartRead) as ei:
        list(slot)
    assert isinstance(ei.value.cause, PostgresRuntimeException)
    assert ei.value.cause.api_error is PostgresErrorCode.POSTGRES_5
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_postgres_6(
    monkeypatch,
    work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(queries=["SELECT 1"])
    [slot] = plugin.PostgresSrcPlugin().read_in(
        _src_ctx(work_dir), _bare_src_conn(), src
    )
    with pytest.raises(PostgresRuntimeException) as ei:
        list(slot)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_6
    assert "query_index=0" in str(ei.value)


# ---------------------------------------------------------------------------
# _PostgresDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_postgres_7_on_tables_slots_mismatch(work_dir):
    dest = PostgresDest(tables=["users", "orders"])
    src = pl.LazyFrame([{"id": 1}])
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
    src = pl.LazyFrame([{"id": 1}])
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
    src = pl.LazyFrame([{"id": 1}])
    dest = PostgresDest(tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_dest_conn(), dest, [src]
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_9


def test_dest_plugin_leaves_the_table_of_a_none_slot_untouched(monkeypatch, work_dir):
    written: list[str] = []
    monkeypatch.setattr(impl, "build_postgres_engine", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(
        saw_writer.PolarsWriteEngine,
        "loader",
        lambda self: lambda **options: written.append(options["table"]),
    )
    dest = PostgresDest(
        tables=["users", "orders", "events"],
        if_table_exists="replace",
        transactional=False,
    )
    plugin.PostgresDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_dest_conn(),
        dest,
        [pl.LazyFrame([{"id": 1}]), None, pl.LazyFrame([{"id": 3}])],
    )
    assert written == ["users", "events"]


def test_dest_plugin_writes_nothing_when_every_slot_is_none(monkeypatch, work_dir):
    build = MagicMock()
    monkeypatch.setattr(impl, "build_postgres_engine", build)
    plugin.PostgresDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_dest_conn(),
        PostgresDest(tables=["users", "orders"], if_table_exists="replace"),
        [None, None],
    )
    build.assert_not_called()


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
    assert cfg.drop_metadata is False


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg({
        "postgres.logging_level": 20,
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
        "postgres.logging_level": 20,
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
# ``PostgresSrcPlugin.read_in`` actually feeds its result into
# ``PostgresReadCfg`` rather than the raw Src field.
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
    src = PostgresSrc(
        queries=["SELECT * FROM t WHERE ts > :last_modified"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin.PostgresSrcPlugin().read_in(ctx, _bare_src_conn(), src)
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
    src = PostgresSrc(queries=["SELECT * FROM t"])
    plugin.PostgresSrcPlugin().read_in(CtxStub(work_dir), conn, src)
    assert captured == {
        "faults": SqlReadFaults(
            unreachable=PostgresErrorCode.POSTGRES_5,
            failed=PostgresErrorCode.POSTGRES_6,
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

    src = PostgresSrc(
        queries=["SELECT * FROM t"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    plugin.PostgresSrcPlugin().read_in(CtxStub(work_dir), _bare_src_conn(), src)
    assert captured["drop_metadata"] is True
