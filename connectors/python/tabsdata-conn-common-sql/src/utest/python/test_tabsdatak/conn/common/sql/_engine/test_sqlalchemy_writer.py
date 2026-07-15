from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_writer as saw

from test_tabsdatak.conn.common.sql import _sqlite_helpers
from test_tabsdatak.conn.common.sql._sqlite_helpers import (
    _SqliteSqlWriteCfg,
    sqlite_uri,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{p}")
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, "
                "name TEXT, age INTEGER)"
            ))
            conn.execute(text(
                "INSERT INTO users (id, name, age) VALUES "
                "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"
            ))
            conn.execute(text(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, "
                "user_id INTEGER, total REAL)"
            ))
            conn.execute(text(
                "INSERT INTO orders (id, user_id, total) VALUES "
                "(10, 1, 9.99), (11, 2, 14.50)"
            ))
    finally:
        engine.dispose()
    return p


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _cfg(db_path: Path, **kwargs) -> _SqliteSqlWriteCfg:
    kwargs.setdefault("target_tables", ["users"])
    return _SqliteSqlWriteCfg(uri=sqlite_uri(db_path), **kwargs)


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    df = pl.DataFrame(rows)
    df.write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch):
    monkeypatch.setattr(saw, "RETRY_WAIT_S", 0)
    monkeypatch.setattr(saw, "RETRY_TIMEOUT_S", 1)


# ---------------------------------------------------------------------------
# PolarsWriteEngine: happy paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transactional", [True, False])
def test_polars_single_table_append(db_path, work_dir, transactional):
    src = _make_parquet(
        work_dir / "in0.parquet",
        [
            {"id": 100, "name": "dave", "age": 50},
            {"id": 101, "name": "eve", "age": 22},
        ],
    )
    saw.PolarsWriteEngine().write(
        _cfg(db_path, transactional=transactional),
        [src],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one()
            assert n == 5
    finally:
        engine.dispose()


def test_polars_replace_drops_existing_rows(db_path, work_dir):
    src = _make_parquet(
        work_dir / "in0.parquet",
        [{"id": 1, "name": "only"}],
    )
    saw.PolarsWriteEngine().write(
        _cfg(db_path, if_table_exists="replace"),
        [src],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM users")
            ).fetchall()
            assert rows == [(1, "only")]
    finally:
        engine.dispose()


def test_polars_replace_chunked_does_not_truncate_itself(db_path, work_dir):
    # Multi-chunk replace: first batch replaces, subsequent batches
    # must append -- otherwise batch N+1 wipes batch N. Drive
    # chunk_size=1 via the cfg dict.
    src = _make_parquet(
        work_dir / "in0.parquet",
        [
            {"id": 100, "name": "x"},
            {"id": 101, "name": "y"},
            {"id": 102, "name": "z"},
        ],
    )
    saw.PolarsWriteEngine().write(
        _cfg(
            db_path,
            if_table_exists="replace",
            cfg={const.WRITE_CHUNK_SIZE_KEY: 1},
        ),
        [src],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            ids = [r[0] for r in conn.execute(
                text("SELECT id FROM users ORDER BY id")
            ).fetchall()]
            assert ids == [100, 101, 102]
    finally:
        engine.dispose()


def test_polars_multi_table(db_path, work_dir):
    src_u = _make_parquet(
        work_dir / "u.parquet",
        [{"id": 1000, "name": "ZZ"}],
    )
    src_o = _make_parquet(
        work_dir / "o.parquet",
        [{"id": 999, "user_id": 1000, "total": 1.0}],
    )
    saw.PolarsWriteEngine().write(
        _cfg(db_path, target_tables=["users", "orders"]),
        [src_u, src_o],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT name FROM users WHERE id=1000"
            )).scalar_one() == "ZZ"
            assert conn.execute(text(
                "SELECT total FROM orders WHERE id=999"
            )).scalar_one() == 1.0
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Empty-parquet handling -- writer must still preserve / create the
# target table even when the input is empty.
# ---------------------------------------------------------------------------


def test_polars_write_empty_parquet_creates_table_with_replace(db_path, work_dir):
    empty_pq = work_dir / "empty.parquet"
    pl.DataFrame(
        {"id": pl.Series([], dtype=pl.Int64),
         "name": pl.Series([], dtype=pl.Utf8)},
    ).write_parquet(empty_pq, use_pyarrow=True)

    saw.PolarsWriteEngine().write(
        _cfg(
            db_path,
            target_tables=["empty_replace"],
            if_table_exists="replace",
        ),
        [empty_pq],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            n = conn.execute(
                text("SELECT COUNT(*) FROM empty_replace")
            ).scalar_one()
            assert n == 0
            cols = [
                r[1] for r in conn.execute(
                    text("PRAGMA table_info(empty_replace)")
                ).fetchall()
            ]
            assert {"id", "name"}.issubset(cols)
    finally:
        engine.dispose()


def test_polars_write_empty_parquet_appends_zero_rows(db_path, work_dir):
    empty_pq = work_dir / "empty.parquet"
    pl.DataFrame(
        {"id": pl.Series([], dtype=pl.Int64),
         "name": pl.Series([], dtype=pl.Utf8)},
    ).write_parquet(empty_pq, use_pyarrow=True)

    saw.PolarsWriteEngine().write(
        _cfg(db_path, if_table_exists="append"),
        [empty_pq],
    )
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one()
            assert n == 3  # original 3, no rows added
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Cardinality mismatch -- generic ValueError
# ---------------------------------------------------------------------------


def test_cardinality_mismatch_raises_value_error(db_path, work_dir):
    src = _make_parquet(work_dir / "x.parquet", [{"id": 1}])
    with pytest.raises(ValueError, match="tables count"):
        saw.PolarsWriteEngine().write(
            _cfg(db_path, target_tables=["a", "b"]),
            [src],
        )


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


def _operational(msg: str) -> OperationalError:
    return OperationalError(msg, {}, BaseException(msg))


def _programming(msg: str) -> ProgrammingError:
    return ProgrammingError(msg, {}, BaseException(msg))


def test_operational_error_on_write_retries(monkeypatch, tmp_path: Path):
    attempts = {"n": 0}

    def fake_build_engine(*a, **kw):
        attempts["n"] += 1
        raise _operational("write connect failed")

    monkeypatch.setattr(_sqlite_helpers, "build_engine", fake_build_engine)

    parquet = tmp_path / "x.parquet"
    parquet.write_bytes(b"")
    with pytest.raises(OperationalError):
        saw.PolarsWriteEngine().write(
            _SqliteSqlWriteCfg(
                uri=f"sqlite:///{tmp_path / 'unused.db'}",
                target_tables=["t"],
            ),
            [parquet],
        )
    assert attempts["n"] >= 2


def test_engine_disposed_after_write_failure(monkeypatch, tmp_path: Path):
    fake_engine = MagicMock()
    fake_engine.connect.side_effect = _programming("nope")

    monkeypatch.setattr(
        _sqlite_helpers, "build_engine",
        lambda *a, **kw: fake_engine,
    )
    parquet = tmp_path / "x.parquet"
    parquet.write_bytes(b"")
    with pytest.raises(ProgrammingError):
        saw.PolarsWriteEngine().write(
            _SqliteSqlWriteCfg(
                uri=f"sqlite:///{tmp_path / 'unused.db'}",
                target_tables=["t"],
                transactional=False,
            ),
            [parquet],
        )
    fake_engine.dispose.assert_called()


# ---------------------------------------------------------------------------
# ibm_db (DB2) rowcount guard
# ---------------------------------------------------------------------------


class _RowcountForbidden:
    """A DBAPI result whose ``rowcount`` read raises, mirroring ibm_db's
    CLI0125E 'Function sequence error' after an executemany INSERT."""

    @property
    def rowcount(self):
        raise AssertionError("rowcount must not be read for ibm_db")


def test_write_database_options_only_for_ibm_db_sa():
    ibm = MagicMock()
    ibm.dialect.name = "ibm_db_sa"
    assert saw._write_database_options(ibm) == {
        "engine_options": {"method": saw._insert_without_rowcount}
    }
    for other in ("sqlite", "postgresql", "mysql", "oracle"):
        conn = MagicMock()
        conn.dialect.name = other
        assert saw._write_database_options(conn) == {}


def test_insert_without_rowcount_skips_rowcount_read():
    pd_table = MagicMock()
    conn = MagicMock()
    conn.execute.return_value = _RowcountForbidden()
    out = saw._insert_without_rowcount(pd_table, conn, ["id"], iter([(1,), (2,)]))
    assert out is None
    conn.execute.assert_called_once()
