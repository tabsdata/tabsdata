from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak.conn.common.types import UserPassword
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import (
    sqlalchemy_reader as saw_reader,
    sqlalchemy_writer as saw_writer,
)
from tabsdatak.conn.postgres import PostgresConn, PostgresDest, PostgresSrc
# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _adapter as adapter
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

    def work_dir(self) -> Path:
        return self._work


def _src_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _conn_from(cfg) -> PostgresConn:
    """Build a PostgresConn pointing at the docker container. The URI is
    ``postgresql+psycopg2://`` so the adapter's ``add_driver_to_uri`` is
    a no-op and psycopg2 (which IS installed in the dev env) is used."""
    return PostgresConn(
        uri=cfg.uri,
        credentials=cfg.credentials,
    )


def _read_parquet(p: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(p))


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
# _PostgresSrcPlugin.read_in -- happy path against real Postgres
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(fresh_postgres, work_dir):
    src = PostgresSrc(
        conn=_conn_from(fresh_postgres),
        queries=["SELECT id, name FROM users ORDER BY id"],
    )
    out = plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["name"].to_list() == ["alice", "bob", "carol"]


def test_src_plugin_read_in_multiple_queries(fresh_postgres, work_dir):
    src = PostgresSrc(
        conn=_conn_from(fresh_postgres),
        queries=[
            "SELECT id FROM users WHERE age >= :cut ORDER BY id",
            "SELECT id FROM orders ORDER BY id",
        ],
        query_params={"cut": 30},
    )
    out = plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 2
    ids = pl.concat([_read_parquet(p) for p in out[0]])["id"].to_list()
    order_ids = pl.concat([_read_parquet(p) for p in out[1]])["id"].to_list()
    assert ids == [1, 3]
    assert order_ids == [10, 11]


# ---------------------------------------------------------------------------
# _PostgresSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# We mock ``build_postgres_engine`` to return a MagicMock whose
# ``connect`` raises the desired exception. Mocking is necessary
# here -- a real engine can't be forced to surface a specific
# OperationalError on demand.
# ---------------------------------------------------------------------------


def _bare_conn() -> PostgresConn:
    """Minimal PostgresConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return PostgresConn(
        uri="postgresql://stub.invalid:5432/db",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_postgres_7(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_7
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_postgres_8(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_postgres_engine", lambda *a, **kw: fake)
    src = PostgresSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_8


# ---------------------------------------------------------------------------
# _PostgresDestPlugin.write_out -- happy path against real Postgres
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(fresh_postgres, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 100, "name": "dave", "age": 50}],
    )
    dest = PostgresDest(
        conn=_conn_from(fresh_postgres),
        tables=["users"],
    )
    plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_postgres.uri_with_credentials)
    try:
        with eng.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one()
            assert n == 4
            name = conn.execute(
                text("SELECT name FROM users WHERE id = 100"),
            ).scalar_one()
            assert name == "dave"
    finally:
        eng.dispose()


def test_dest_plugin_write_out_replace_clears_existing(fresh_postgres, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "only", "age": 1}],
    )
    dest = PostgresDest(
        conn=_conn_from(fresh_postgres),
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_postgres.uri_with_credentials)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users"),
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# _PostgresDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_postgres_10_on_tables_slots_mismatch(work_dir):
    dest = PostgresDest(conn=_bare_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_10


def test_dest_plugin_wraps_operational_error_as_postgres_11(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_11
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_postgres_9(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_postgres_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = PostgresDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(PostgresRuntimeException) as ei:
        plugin.PostgresDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_9


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.PostgresExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_postgres_src_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_SRC.conn is PostgresConn
    assert plugin.POSTGRES_SRC.src is PostgresSrc
    assert plugin.POSTGRES_SRC.plugin is plugin.PostgresSrcPlugin
    assert plugin.POSTGRES_SRC.explorer is plugin.PostgresExplorer
    assert plugin.POSTGRES_SRC.src_type == "postgres"


def test_postgres_dest_def_wires_plugin_and_explorer():
    assert plugin.POSTGRES_DEST.conn is PostgresConn
    assert plugin.POSTGRES_DEST.dest is PostgresDest
    assert plugin.POSTGRES_DEST.plugin is plugin.PostgresDestPlugin
    assert plugin.POSTGRES_DEST.explorer is plugin.PostgresExplorer
    assert plugin.POSTGRES_DEST.dest_type == "postgres"


def test_postgres_src_cardinality_uses_queries_length():
    src = PostgresSrc(conn=_bare_conn(), queries=["SELECT 1", "SELECT 2", "SELECT 3"])
    assert plugin.POSTGRES_SRC.cardinality(src) == 3


def test_postgres_dest_cardinality_uses_tables_length():
    dest = PostgresDest(conn=_bare_conn(), tables=["users", "orders"])
    assert plugin.POSTGRES_DEST.cardinality(dest) == 2
