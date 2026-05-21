"""End-to-end StarRocks plugin tests against a real StarRocks instance.

The ``fresh_starrocks`` fixture (defined in ``conftest.py``) provisions a
clean StarRocks backend per test -- either a per-session docker container
(via the ``starrocks/allin1-ubuntu`` image) or an externally-configured
server when the relevant environment knobs are set. These tests exercise
the actual ``StarRocksDestPlugin.write_out`` round-trip end-to-end;
error-path coverage (fault-injection via mocks) lives in
``test__plugin.py``.

Because StarRocks bulk loading routes through cloud staging, these tests
*also* require a usable :class:`S3Location` staging area. The fixture
takes those settings from ``STARROCKS0__S3_STAGING_*`` env vars; when
they aren't set the integration tests are skipped.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from tabsdatak.conn.starrocks import StarRocksConn, StarRocksDest
# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _plugin as plugin
from tabsdatak.spi import DestContext


pytestmark = pytest.mark.starrocks


# ---------------------------------------------------------------------------
# Test context shims -- concrete DestContext subclass so the integration
# tests exercise the real ``DestContext`` protocol rather than a duck-typed
# surrogate.
# ---------------------------------------------------------------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "starrocks-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "starrocks-integ-trx"

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

def _dest_ctx(work: Path) -> _DestCtx:
    return _DestCtx(work)


def _conn_from(cfg, staging) -> StarRocksConn:
    """Build a StarRocksConn pointing at the ``fresh_starrocks`` backend
    and the configured S3 staging area."""
    return StarRocksConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        staging=staging,
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
# StarRocksDestPlugin.write_out -- happy path against real StarRocks
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_appends_rows(
    fresh_starrocks, s3_staging, work_dir,
):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 100, "name": "dave", "age": 50}],
    )
    dest = StarRocksDest(
        conn=_conn_from(fresh_starrocks, s3_staging),
        tables=["users"],
    )
    plugin.StarRocksDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_starrocks.uri_with_credentials)
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


def test_dest_plugin_write_out_replace_clears_existing(
    fresh_starrocks, s3_staging, work_dir,
):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "only", "age": 1}],
    )
    dest = StarRocksDest(
        conn=_conn_from(fresh_starrocks, s3_staging),
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.StarRocksDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    eng = create_engine(fresh_starrocks.uri_with_credentials)
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users"),
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        eng.dispose()


def test_dest_plugin_write_out_two_tables_round_trip(
    fresh_starrocks, s3_staging, work_dir,
):
    users = _make_parquet(
        work_dir / "users.parquet",
        [{"id": 200, "name": "eve", "age": 22}],
    )
    orders = _make_parquet(
        work_dir / "orders.parquet",
        [{"id": 99, "user_id": 200, "qty": 1}],
    )
    dest = StarRocksDest(
        conn=_conn_from(fresh_starrocks, s3_staging),
        tables=["users", "orders"],
    )
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), dest, [users, orders],
    )
    eng = create_engine(fresh_starrocks.uri_with_credentials)
    try:
        with eng.connect() as conn:
            u_name = conn.execute(
                text("SELECT name FROM users WHERE id = 200"),
            ).scalar_one()
            o_qty = conn.execute(
                text("SELECT qty FROM orders WHERE id = 99"),
            ).scalar_one()
            assert u_name == "eve"
            assert o_qty == 1
    finally:
        eng.dispose()
