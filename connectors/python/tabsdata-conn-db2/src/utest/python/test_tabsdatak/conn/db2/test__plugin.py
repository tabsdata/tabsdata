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
from tabsdatak.conn.db2 import Db2Conn, Db2Dest, Db2Src
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _adapter as adapter
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _plugin as plugin
from tabsdatak.conn.db2.error import (
    Db2RuntimeException,
    Db2ErrorCode,
)


pytestmark = pytest.mark.db2


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


def _conn_from(cfg) -> Db2Conn:
    """Build a Db2Conn pointing at the docker container. The URI is
    ``db2+ibm_db://`` so the adapter's ``add_driver_to_uri`` is a
    no-op and ibm_db (which IS installed in the dev env) is used."""
    return Db2Conn(
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
# _Db2SrcPlugin.read_in -- happy path against real Db2
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(fresh_db2, work_dir):
    src = Db2Src(
        conn=_conn_from(fresh_db2),
        queries=["SELECT id, name FROM users ORDER BY id"],
    )
    out = plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    # Db2 folds unquoted identifiers to upper-case.
    name_col = "NAME" if "NAME" in df.columns else "name"
    assert df[name_col].to_list() == ["alice", "bob", "carol"]


def test_src_plugin_read_in_multiple_queries(fresh_db2, work_dir):
    src = Db2Src(
        conn=_conn_from(fresh_db2),
        queries=[
            "SELECT id FROM users WHERE age >= :cut ORDER BY id",
            "SELECT id FROM orders ORDER BY id",
        ],
        query_params={"cut": 30},
    )
    out = plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 2
    df0 = pl.concat([_read_parquet(p) for p in out[0]])
    df1 = pl.concat([_read_parquet(p) for p in out[1]])
    id_col_0 = "ID" if "ID" in df0.columns else "id"
    id_col_1 = "ID" if "ID" in df1.columns else "id"
    assert df0[id_col_0].to_list() == [1, 3]
    assert df1[id_col_1].to_list() == [10, 11]


# ---------------------------------------------------------------------------
# _Db2SrcPlugin.read_in -- error wrapping (fault injection via mocks)
#
# We mock ``build_db2_engine`` to return a MagicMock whose
# ``connect`` raises the desired exception. Mocking is necessary
# here -- a real engine can't be forced to surface a specific
# OperationalError on demand.
# ---------------------------------------------------------------------------


def _bare_conn() -> Db2Conn:
    """Minimal Db2Conn for error tests -- the URI/creds never see
    a real driver (the engine is mocked)."""
    from tabsdatak.api import Secret
    return Db2Conn(
        uri="db2://stub.invalid:50000/db",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_src_plugin_wraps_operational_error_as_db2_7(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(conn=_bare_conn(), queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is Db2ErrorCode.DB2_7
    fake.dispose.assert_called()


def test_src_plugin_wraps_other_sqlalchemy_error_as_db2_8(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_db2_engine", lambda *a, **kw: fake)
    src = Db2Src(conn=_bare_conn(), queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2SrcPlugin().read_in(_src_ctx(work_dir), src)
    assert ei.value.api_error is Db2ErrorCode.DB2_8


# ---------------------------------------------------------------------------
# _Db2DestPlugin.write_out -- happy path against real Db2
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(fresh_db2, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 100, "name": "dave", "age": 50}],
    )
    dest = Db2Dest(
        conn=_conn_from(fresh_db2),
        tables=["users"],
    )
    plugin.Db2DestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_db2.uri_with_credentials)
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


def test_dest_plugin_write_out_replace_clears_existing(fresh_db2, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "only", "age": 1}],
    )
    dest = Db2Dest(
        conn=_conn_from(fresh_db2),
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.Db2DestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_db2.uri_with_credentials)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users"),
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# _Db2DestPlugin.write_out -- error paths (fault injection)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_db2_10_on_tables_slots_mismatch(work_dir):
    dest = Db2Dest(conn=_bare_conn(), tables=["users", "orders"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_10


def test_dest_plugin_wraps_operational_error_as_db2_11(monkeypatch, work_dir):
    fake = MagicMock()
    fake.connect.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(adapter, "build_db2_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = Db2Dest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is Db2ErrorCode.DB2_11
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_db2_9(
    monkeypatch, work_dir,
):
    fake = MagicMock()
    fake.connect.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(adapter, "build_db2_engine", lambda *a, **kw: fake)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = Db2Dest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(Db2RuntimeException) as ei:
        plugin.Db2DestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is Db2ErrorCode.DB2_9


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.Db2Explorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1 FROM SYSIBM.SYSDUMMY1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_db2_src_def_wires_plugin_and_explorer():
    assert plugin.DB2_SRC.conn is Db2Conn
    assert plugin.DB2_SRC.src is Db2Src
    assert plugin.DB2_SRC.plugin is plugin.Db2SrcPlugin
    assert plugin.DB2_SRC.explorer is plugin.Db2Explorer
    assert plugin.DB2_SRC.src_type == "db2"


def test_db2_dest_def_wires_plugin_and_explorer():
    assert plugin.DB2_DEST.conn is Db2Conn
    assert plugin.DB2_DEST.dest is Db2Dest
    assert plugin.DB2_DEST.plugin is plugin.Db2DestPlugin
    assert plugin.DB2_DEST.explorer is plugin.Db2Explorer
    assert plugin.DB2_DEST.dest_type == "db2"


def test_db2_src_cardinality_uses_queries_length():
    src = Db2Src(
        conn=_bare_conn(),
        queries=[
            "SELECT 1 FROM SYSIBM.SYSDUMMY1",
            "SELECT 2 FROM SYSIBM.SYSDUMMY1",
            "SELECT 3 FROM SYSIBM.SYSDUMMY1",
        ],
    )
    assert plugin.DB2_SRC.cardinality(src) == 3


def test_db2_dest_cardinality_uses_tables_length():
    dest = Db2Dest(conn=_bare_conn(), tables=["users", "orders"])
    assert plugin.DB2_DEST.cardinality(dest) == 2
