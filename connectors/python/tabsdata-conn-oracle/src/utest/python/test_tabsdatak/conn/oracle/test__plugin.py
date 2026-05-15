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
from tabsdatak.conn.oracle import OracleConn, OracleDest, OracleSrc
# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _adapter as adapter
# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _plugin as plugin
from tabsdatak.conn.oracle.error import (
    OracleRuntimeException,
    OracleErrorCode,
)


pytestmark = pytest.mark.oracle


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


def _conn_from(cfg) -> OracleConn:
    """Build an OracleConn pointing at the docker container."""
    return OracleConn(
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
# _OracleSrcPlugin.read_in -- happy path against real Oracle
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(fresh_oracle, work_dir):
    src = OracleSrc(
        conn=_conn_from(fresh_oracle),
        queries=["SELECT id, name FROM users ORDER BY id"],
    )
    out = plugin.OracleSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    names = df["name"].to_list() if "name" in df.columns else df["NAME"].to_list()
    assert names == ["alice", "bob", "carol"]


def test_src_plugin_read_in_multiple_queries(fresh_oracle, work_dir):
    src = OracleSrc(
        conn=_conn_from(fresh_oracle),
        queries=[
            "SELECT id FROM users WHERE age >= :cut ORDER BY id",
            "SELECT id FROM orders ORDER BY id",
        ],
        query_params={"cut": 30},
    )
    out = plugin.OracleSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 2
    users_df = pl.concat([_read_parquet(p) for p in out[0]])
    orders_df = pl.concat([_read_parquet(p) for p in out[1]])
    id_col_u = "id" if "id" in users_df.columns else "ID"
    id_col_o = "id" if "id" in orders_df.columns else "ID"
    assert users_df[id_col_u].to_list() == [1, 3]
    assert orders_df[id_col_o].to_list() == [10, 11]


# ---------------------------------------------------------------------------
# _OracleSrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# We mock ``build_oracle_engine`` to return a MagicMock whose
# ``connect`` raises the desired exception. Mocking is necessary
# here -- a real engine can't be forced to surface a specific
# OperationalError on demand.
# ---------------------------------------------------------------------------


def _bare_conn() -> OracleConn:
    """Minimal OracleConn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return OracleConn(
        uri="oracle://stub.invalid:1521/orcl",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_oracle_7(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_oracle_engine", lambda *a, **kw: fake)
    src = OracleSrc(conn=_bare_conn(), queries=["SELECT 1 FROM DUAL"])
    with pytest.raises(OracleRuntimeException) as ei:
        plugin.OracleSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is OracleErrorCode.ORACLE_7
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_oracle_8(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_oracle_engine", lambda *a, **kw: fake)
    src = OracleSrc(conn=_bare_conn(), queries=["SELECT 1 FROM DUAL"])
    with pytest.raises(OracleRuntimeException) as ei:
        plugin.OracleSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is OracleErrorCode.ORACLE_8


# ---------------------------------------------------------------------------
# _OracleDestPlugin.write_out -- happy path against real Oracle
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(fresh_oracle, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"ID": 100, "NAME": "dave", "AGE": 50}],
    )
    dest = OracleDest(
        conn=_conn_from(fresh_oracle),
        tables=["users"],
    )
    plugin.OracleDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_oracle.uri_with_credentials)
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


def test_dest_plugin_write_out_replace_clears_existing(fresh_oracle, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"ID": 1, "NAME": "only", "AGE": 1}],
    )
    dest = OracleDest(
        conn=_conn_from(fresh_oracle),
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.OracleDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_oracle.uri_with_credentials)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users"),
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# _OracleDestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_oracle_10_on_tables_slots_mismatch(work_dir):
    dest = OracleDest(conn=_bare_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(OracleRuntimeException) as ei:
        plugin.OracleDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_10


def test_dest_plugin_wraps_operational_error_as_oracle_11(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_oracle_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = OracleDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(OracleRuntimeException) as ei:
        plugin.OracleDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is OracleErrorCode.ORACLE_11
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_oracle_9(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_oracle_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = OracleDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(OracleRuntimeException) as ei:
        plugin.OracleDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is OracleErrorCode.ORACLE_9


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.OracleExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1 FROM DUAL") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_oracle_src_def_wires_plugin_and_explorer():
    assert plugin.ORACLE_SRC.conn is OracleConn
    assert plugin.ORACLE_SRC.src is OracleSrc
    assert plugin.ORACLE_SRC.plugin is plugin.OracleSrcPlugin
    assert plugin.ORACLE_SRC.explorer is plugin.OracleExplorer
    assert plugin.ORACLE_SRC.src_type == "oracle"


def test_oracle_dest_def_wires_plugin_and_explorer():
    assert plugin.ORACLE_DEST.conn is OracleConn
    assert plugin.ORACLE_DEST.dest is OracleDest
    assert plugin.ORACLE_DEST.plugin is plugin.OracleDestPlugin
    assert plugin.ORACLE_DEST.explorer is plugin.OracleExplorer
    assert plugin.ORACLE_DEST.dest_type == "oracle"


def test_oracle_src_cardinality_uses_queries_length():
    src = OracleSrc(
        conn=_bare_conn(),
        queries=["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL", "SELECT 3 FROM DUAL"],
    )
    assert plugin.ORACLE_SRC.cardinality(src) == 3


def test_oracle_dest_cardinality_uses_tables_length():
    dest = OracleDest(conn=_bare_conn(), tables=["users", "orders"])
    assert plugin.ORACLE_DEST.cardinality(dest) == 2
