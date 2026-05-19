
from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.db2 import Db2Conn, Db2Dest, Db2Src
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _plugin as plugin
from tabsdatak.spi import DestContext, SrcContext


pytestmark = pytest.mark.db2


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
        return "db2-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "db2-integ-trx"

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
        return "db2-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "db2-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


def _src_ctx(work: Path) -> _SrcCtx:
    return _SrcCtx(work)


def _dest_ctx(work: Path) -> _DestCtx:
    return _DestCtx(work)


def _conn_from(cfg) -> Db2Conn:
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


# ---------------------------------------------------------------------------
# Db2SrcPlugin.read_in -- happy path against real Db2
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
# Db2DestPlugin.write_out -- happy path against real Db2
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
