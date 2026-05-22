"""End-to-end Snowflake integration tests: write parquet files to
real Snowflake tables via SQLAlchemy. Skipped unless ALL of these
env vars are set:

  - ``SNW__ACCOUNT``    -- Snowflake account identifier
  - ``SNW__USER``       -- Snowflake user
  - ``SNW__PAT``        -- Snowflake Personal Access Token (used as password)
  - ``SNW__DATABASE``   -- target database
  - ``SNW__SCHEMA``     -- target schema
  - ``SNW__WAREHOUSE``  -- warehouse to use
  - ``SNW__ROLE``       -- (optional) role to assume

Each test creates a uniquely-named table under the configured schema
so concurrent runs don't collide.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn, SnowflakeDest

# noinspection PyProtectedMember
import tabsdatak.conn.snowflake._plugin as plugin
from tabsdatak.spi import DestContext


pytestmark = pytest.mark.snowflake_live


_ACCOUNT = os.environ.get("SNW__ACCOUNT")
_USER = os.environ.get("SNW__USER")
_PASSWORD = os.environ.get("SNW__PAT")
_DATABASE = os.environ.get("SNW__DATABASE")
_SCHEMA = os.environ.get("SNW__SCHEMA")
_WAREHOUSE = os.environ.get("SNW__WAREHOUSE")
_ROLE = os.environ.get("SNW__ROLE")


_skip_reason = (
    "Snowflake live integration tests require SNW__ACCOUNT + "
    "SNW__USER + SNW__PAT + SNW__DATABASE + "
    "SNW__SCHEMA + SNW__WAREHOUSE"
)


def _gates_ok() -> bool:
    return all([_ACCOUNT, _USER, _PASSWORD, _DATABASE, _SCHEMA, _WAREHOUSE])


pytestmark = [pytestmark, pytest.mark.skipif(not _gates_ok(), reason=_skip_reason)]


# ---------------------------------------------------------------------------
# Test contexts
# ---------------------------------------------------------------------------


class _SrcCtx:
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "sf-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "sf-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "sf-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "sf-integ-trx"

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

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn() -> SnowflakeDestConn:
    kwargs: dict = dict(
        account=_ACCOUNT,
        credentials=UserPassword(
            user=Secret.lit(_USER),
            password=Secret.lit(_PASSWORD),
        ),
        database=_DATABASE,
        schema=_SCHEMA,
        warehouse=_WAREHOUSE,
    )
    if _ROLE:
        kwargs["role"] = _ROLE
    return SnowflakeDestConn(**kwargs)


@pytest.fixture
def run_table() -> str:
    """A unique table name for each test, so parallel runs don't
    clash."""
    return f"TD_INTEG_{uuid.uuid4().hex[:12].upper()}"


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


# ---------------------------------------------------------------------------
# write_out
# ---------------------------------------------------------------------------


def _drop_table(conn: SnowflakeDestConn, fq_table: str) -> None:
    # noinspection PyProtectedMember
    from tabsdatak.conn.snowflake._impl import build_snowflake_connection

    sf_conn = build_snowflake_connection(conn)
    try:
        with sf_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {fq_table}")
    finally:
        sf_conn.close()


def _select_rows(conn: SnowflakeDestConn, fq_table: str) -> list[tuple]:
    # noinspection PyProtectedMember
    from tabsdatak.conn.snowflake._impl import build_snowflake_connection

    sf_conn = build_snowflake_connection(conn)
    try:
        with sf_conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {fq_table} ORDER BY id")
            return list(cur.fetchall())
    finally:
        sf_conn.close()


def test_dest_plugin_write_out_appends_rows(conn, run_table, work_dir):
    fq = f"{_DATABASE}.{_SCHEMA}.{run_table}"
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    dest = SnowflakeDest(conn=conn, tables=[fq], if_table_exists="replace")
    try:
        plugin.SnowflakeDestPlugin().write_out(_DestCtx(work_dir), dest, [src])
        rows = _select_rows(conn, fq)
        assert [(r[0], r[1]) for r in rows] == [(1, "alice"), (2, "bob")]
    finally:
        _drop_table(conn, fq)


def test_dest_plugin_write_out_replace_clears_existing(conn, run_table, work_dir):
    fq = f"{_DATABASE}.{_SCHEMA}.{run_table}"
    src1 = _make_parquet(
        work_dir / "in1.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    src2 = _make_parquet(
        work_dir / "in2.parquet",
        [{"id": 99, "name": "only"}],
    )
    try:
        plugin.SnowflakeDestPlugin().write_out(
            _DestCtx(work_dir),
            SnowflakeDest(conn=conn, tables=[fq], if_table_exists="replace"),
            [src1],
        )
        plugin.SnowflakeDestPlugin().write_out(
            _DestCtx(work_dir),
            SnowflakeDest(conn=conn, tables=[fq], if_table_exists="replace"),
            [src2],
        )
        rows = _select_rows(conn, fq)
        assert [(r[0], r[1]) for r in rows] == [(99, "only")]
    finally:
        _drop_table(conn, fq)
