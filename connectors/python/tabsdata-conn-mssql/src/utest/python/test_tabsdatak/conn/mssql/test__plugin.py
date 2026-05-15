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
from tabsdatak.conn.mssql import MSSQLConn, MSSQLDest, MSSQLSrc
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _adapter as adapter
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

    def work_dir(self) -> Path:
        return self._work


def _src_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _conn_from(cfg) -> MSSQLConn:
    """Build an MSSQLConn pointing at the docker container. The URI is
    the fixture's driver-qualified ``mssql+pyodbc://`` so the adapter's
    ``add_driver_to_uri`` is a no-op."""
    return MSSQLConn(
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
# _MSSQLSrcPlugin.read_in -- happy path against real MSSQL
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(fresh_mssql, work_dir):
    src = MSSQLSrc(
        conn=_conn_from(fresh_mssql),
        queries=["SELECT id, name FROM users ORDER BY id"],
    )
    out = plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["name"].to_list() == ["alice", "bob", "carol"]


def test_src_plugin_read_in_multiple_queries(fresh_mssql, work_dir):
    src = MSSQLSrc(
        conn=_conn_from(fresh_mssql),
        queries=[
            "SELECT id FROM users WHERE age >= :cut ORDER BY id",
            "SELECT id FROM orders ORDER BY id",
        ],
        query_params={"cut": 30},
    )
    out = plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 2
    ids = pl.concat([_read_parquet(p) for p in out[0]])["id"].to_list()
    order_ids = pl.concat([_read_parquet(p) for p in out[1]])["id"].to_list()
    assert ids == [1, 3]
    assert order_ids == [10, 11]


# ---------------------------------------------------------------------------
# _MSSQLSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# We mock ``build_mssql_engine`` to return a MagicMock whose
# ``connect`` raises the desired exception. Mocking is necessary
# here -- a real engine can't be forced to surface a specific
# OperationalError on demand.
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


def test_src_plugin_wraps_operational_error_as_mssql_6(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_6
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_mssql_7(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_mssql_engine", lambda *a, **kw: fake)
    src = MSSQLSrc(conn=_bare_conn(), queries=["SELECT 1"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_7


# ---------------------------------------------------------------------------
# _MSSQLDestPlugin.write_out -- happy path against real MSSQL
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(fresh_mssql, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 100, "name": "dave", "age": 50}],
    )
    dest = MSSQLDest(
        conn=_conn_from(fresh_mssql),
        tables=["users"],
    )
    plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_mssql.uri_with_credentials)
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


def test_dest_plugin_write_out_replace_clears_existing(fresh_mssql, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "only", "age": 1}],
    )
    dest = MSSQLDest(
        conn=_conn_from(fresh_mssql),
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_mssql.uri_with_credentials)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users"),
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# _MSSQLDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_mssql_9_on_tables_slots_mismatch(work_dir):
    dest = MSSQLDest(conn=_bare_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_9


def test_dest_plugin_wraps_operational_error_as_mssql_10(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_mssql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MSSQLDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is MssqlErrorCode.MSSQL_10
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_mssql_8(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_mssql_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = MSSQLDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(MSSQLRuntimeException) as ei:
        plugin.MSSQLDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is MssqlErrorCode.MSSQL_8


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
