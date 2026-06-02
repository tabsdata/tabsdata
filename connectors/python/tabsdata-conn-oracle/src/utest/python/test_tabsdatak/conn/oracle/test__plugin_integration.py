"""End-to-end Oracle plugin tests against a real Oracle instance.

The ``fresh_oracle`` fixture (defined in ``conftest.py``) provisions a
clean Oracle backend per test -- either a per-session docker container
or an externally-configured server when the relevant environment knobs
are set. These tests exercise the actual ``OracleSrcPlugin.read_in`` /
``OracleDestPlugin.write_out`` round-trips end-to-end; error-path
coverage (fault-injection via mocks) lives in ``test__plugin.py``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.oracle import OracleDestConn, OracleSrcConn, OracleDest, OracleSrc
# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _plugin as plugin
from tabsdatak.spi import DestContext, SrcPluginCtx


pytestmark = [pytest.mark.oracle, pytest.mark.xdist_group("oracle-plugin_integration_seq")]


# ---------------------------------------------------------------------------
# Test context shims -- concrete SrcContext / DestContext subclasses.
# ---------------------------------------------------------------------------


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "oracle-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "oracle-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "oracle-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "oracle-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

def _src_ctx(work: Path) -> _SrcCtx:
    return _SrcCtx(work)


def _dest_ctx(work: Path) -> _DestCtx:
    return _DestCtx(work)


def _src_conn_from(cfg) -> OracleSrcConn:
    """Build an OracleSrcConn pointing at the ``fresh_oracle`` backend."""
    return OracleSrcConn(
        uri=cfg.uri,
        credentials=cfg.credentials,
    )


def _dest_conn_from(cfg) -> OracleDestConn:
    """Build an OracleDestConn pointing at the ``fresh_oracle`` backend."""
    return OracleDestConn(
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
# OracleSrcPlugin.read_in -- happy path against real Oracle
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(fresh_oracle, work_dir):
    src = OracleSrc(
        conn=_src_conn_from(fresh_oracle),
        queries=["SELECT id, name FROM users ORDER BY id"],
    )
    out = plugin.OracleSrcPlugin().read_in(_src_ctx(work_dir), src)
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    names = df["name"].to_list() if "name" in df.columns else df["NAME"].to_list()
    assert names == ["alice", "bob", "carol"]


def test_src_plugin_read_in_multiple_queries(fresh_oracle, work_dir):
    src = OracleSrc(
        conn=_src_conn_from(fresh_oracle),
        queries=[
            "SELECT id FROM users WHERE age >= :cut ORDER BY id",
            "SELECT id FROM orders ORDER BY id",
        ],
        initial_values={"cut": 30},
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
# OracleDestPlugin.write_out -- happy path against real Oracle
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(fresh_oracle, work_dir):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"ID": 100, "NAME": "dave", "AGE": 50}],
    )
    dest = OracleDest(
        conn=_dest_conn_from(fresh_oracle),
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
        conn=_dest_conn_from(fresh_oracle),
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
