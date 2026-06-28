"""Shared helpers for the ClickHouse integration test files.

Lives in a private (underscore-prefixed) module so pytest doesn't
collect it as tests. Both ``test__plugin_integration.py`` and
``test__plugin_iceberg_integration.py`` import from here, and the
per-test fixtures in ``conftest.py`` use these too.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from tabsdatak.conn.clickhouse import ClickHouseDestConn
from tabsdatak.spi import DestContext, SrcPluginCtx


# ---------------------------------------------------------------------------
# Test context shims -- concrete subclasses of the SPI abstract classes so
# integration tests exercise the real protocol rather than a duck-typed
# surrogate.
# ---------------------------------------------------------------------------


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "clickhouse-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "clickhouse-integ-trx"

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
        return "clickhouse-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "clickhouse-integ-trx"

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


# ---------------------------------------------------------------------------
# Connection / parquet helpers
# ---------------------------------------------------------------------------


def _conn_from(cfg) -> ClickHouseDestConn:
    """Build a ClickHouseDestConn from a :class:`conftest.CHConfig`."""
    return ClickHouseDestConn(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        credentials=cfg.credentials,
        staging=cfg.staging,
        secure=cfg.secure,
    )


def _read_parquet(p: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(p))


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    """Write ``rows`` to ``path`` as Parquet and return ``path``.

    Polars infers each column's dtype from the row data. For tests
    that need a specific dtype (e.g. Int32 vs Int64 for type-widening
    checks), build the DataFrame yourself with an explicit
    ``schema=`` and call ``write_parquet(path, use_pyarrow=True)``
    directly.
    """
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


# ---------------------------------------------------------------------------
# ClickHouse-side verification helpers (independent of the plugin)
# ---------------------------------------------------------------------------


def _db_name(fresh_clickhouse) -> str:
    """Extract the database name as a plain string from a
    :class:`conftest.CHConfig`.

    The fixture's ``database`` may be a ``Secret``-shaped value or a
    bare string; this helper normalizes that.
    """
    db = fresh_clickhouse.database
    return db.value if hasattr(db, "value") else db


def _ch_client(fresh_clickhouse):
    """Build a ``clickhouse_connect`` client directly from the
    fixture's connection params -- used by tests to verify ClickHouse
    state without going through the plugin."""
    import clickhouse_connect

    host = fresh_clickhouse.host
    host = host.value() if hasattr(host, "value") else host
    user = fresh_clickhouse.credentials.user.value()
    password = fresh_clickhouse.credentials.password.value()
    return clickhouse_connect.get_client(
        host=host,
        port=fresh_clickhouse.port,
        database=_db_name(fresh_clickhouse),
        username=user,
        password=password,
        secure=getattr(fresh_clickhouse, "secure", False),
    )


def _count_rows(client, db: str, tbl: str) -> int:
    result = client.query(f"SELECT count() FROM `{db}`.`{tbl}`")
    return result.result_rows[0][0]


def _column_types(client, db: str, tbl: str) -> dict[str, str]:
    """Return ``{column_name: clickhouse_type}`` for the named
    table, ordered by position."""
    rows = client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": db, "tbl": tbl},
    ).result_rows
    return {name: typ for name, typ in rows}


def _drop_table(client, db: str, tbl: str) -> None:
    client.command(f"DROP TABLE IF EXISTS `{db}`.`{tbl}`")
