#
# Copyright 2026 Tabsdata Inc.
#

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


def test_polars_replace_does_not_reflect_the_table(db_path, work_dir, monkeypatch):
    # Regression for the Oracle "Could not reflect: requested table(s) not
    # available" flake: 'replace' must drop + append (create-on-write) and never
    # reflect the existing table (pandas' native 'replace' path does).
    import sqlalchemy

    reflected: list = []
    monkeypatch.setattr(
        sqlalchemy.MetaData, "reflect", lambda self, *a, **k: reflected.append(k)
    )
    src = _make_parquet(work_dir / "r.parquet", [{"id": 1, "name": "x"}])
    # An existing table ('users', pre-created by the fixture) ...
    saw.PolarsWriteEngine().write(_cfg(db_path, if_table_exists="replace"), [src])
    # ... and a table that does not exist yet (must create-on-write, not fail).
    saw.PolarsWriteEngine().write(
        _cfg(db_path, target_tables=["brand_new"], if_table_exists="replace"), [src]
    )
    assert reflected == []
    engine = create_engine(sqlite_uri(db_path))
    try:
        with engine.connect() as conn:
            assert (
                conn.execute(text("SELECT COUNT(*) FROM brand_new")).scalar_one() == 1
            )
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


def test_qualify_for_write_qualifies_unqualified_name_on_ibm_db_sa():
    # ibm_db_sa's default_schema_name is the authorization ID, not the CURRENT
    # SCHEMA statements run under, so has_table would probe the wrong schema and
    # miss an existing table. The session is asked instead; DB2 returns the
    # special register space-padded.
    ibm = MagicMock()
    ibm.dialect.name = "ibm_db_sa"
    ibm.execute.return_value.scalar.return_value = "D_TUCU_0_DB2   "
    assert saw._qualify_for_write(ibm, "td_dest") == "D_TUCU_0_DB2.td_dest"


def test_qualify_for_write_leaves_an_already_qualified_name_alone():
    ibm = MagicMock()
    ibm.dialect.name = "ibm_db_sa"
    assert saw._qualify_for_write(ibm, "OTHER.td_dest") == "OTHER.td_dest"
    ibm.execute.assert_not_called()


def test_qualify_for_write_keeps_the_bare_name_when_no_schema_comes_back():
    ibm = MagicMock()
    ibm.dialect.name = "ibm_db_sa"
    ibm.execute.return_value.scalar.return_value = None
    assert saw._qualify_for_write(ibm, "td_dest") == "td_dest"


def test_qualify_for_write_is_a_noop_for_the_other_dialects():
    # Their default_schema_name already is where an unqualified CREATE lands, so
    # nothing is qualified and no extra round trip is spent asking.
    for other in ("sqlite", "postgresql", "mysql", "oracle", "mssql"):
        conn = MagicMock()
        conn.dialect.name = other
        assert saw._qualify_for_write(conn, "td_dest") == "td_dest"
        conn.execute.assert_not_called()


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


def test_unmasked_ibm_db_errors_surfaces_the_real_error(monkeypatch):
    # Fake ibm_db_dbi.Cursor reproducing the driver's masking: a failed
    # executemany runs _set_rowcount() in its except block, which itself raises
    # CLI0125E and thereby replaces the original error.
    import sys
    import types

    class _Cursor:
        def _set_rowcount(self):
            raise RuntimeError("CLI0125E Function sequence error (HY010)")

        def executemany(self):
            try:
                raise ValueError("REAL binding error")
            except Exception as inst:
                self._set_rowcount()  # driver masks the real error here
                raise inst  # reached only when _set_rowcount does not raise

    fake = types.ModuleType("ibm_db_dbi")
    fake.Cursor = _Cursor
    monkeypatch.setitem(sys.modules, "ibm_db_dbi", fake)

    # Unguarded: the cleanup crash (CLI0125E) masks the real cause.
    with pytest.raises(RuntimeError, match="CLI0125E"):
        _Cursor().executemany()

    # Guarded: _set_rowcount's own failure is neutralized, so the real error wins.
    with saw._unmasked_ibm_db_errors():
        with pytest.raises(ValueError, match="REAL binding error"):
            _Cursor().executemany()

    # ...and the driver method is restored on exit.
    with pytest.raises(RuntimeError, match="CLI0125E"):
        _Cursor().executemany()


def test_unmasked_ibm_db_errors_noop_without_driver(monkeypatch):
    # When ibm_db_dbi is not installed, the guard is a transparent no-op.
    import builtins

    real_import = builtins.__import__

    def _no_ibm_db(name, *args, **kwargs):
        if name == "ibm_db_dbi":
            raise ImportError("no ibm_db_dbi")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ibm_db)
    with saw._unmasked_ibm_db_errors():
        pass  # must not raise
