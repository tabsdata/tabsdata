
from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.mssql import MSSQLConn, MSSQLDest, MSSQLSrc
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _plugin as plugin
from tabsdatak.spi import DestContext, SrcContext


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# Test context shims -- concrete SrcContext / DestContext subclasses.
# ---------------------------------------------------------------------------


class _SrcCtx(SrcContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "mssql-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "mssql-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "mssql-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "mssql-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


def _src_ctx(work: Path) -> _SrcCtx:
    return _SrcCtx(work)


def _dest_ctx(work: Path) -> _DestCtx:
    return _DestCtx(work)


def _conn_from(cfg) -> MSSQLConn:
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


# ---------------------------------------------------------------------------
# MSSQLSrcPlugin.read_in -- happy path against real MSSQL
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
# MSSQLDestPlugin.write_out -- happy path against real MSSQL
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
