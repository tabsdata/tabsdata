from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak.conn.common.types import DataType
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_reader as sar

from test_tabsdatak.conn.common.sql import _sqlite_helpers
from test_tabsdatak.conn.common.sql._sqlite_helpers import (
    _SqliteSqlReadCfg,
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
                "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40), "
                "(4, 'dave', 50)"
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


def _read_parquet(p: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(p))


def _cfg(db_path: Path, **kwargs) -> _SqliteSqlReadCfg:
    kwargs.setdefault("queries", ["SELECT 1"])
    return _SqliteSqlReadCfg(uri=sqlite_uri(db_path), **kwargs)


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch):
    # Retries fire on OperationalError. The reader engine creates
    # its own Retrying via the module-level retry helper -- shorten
    # both knobs so the failing-retry tests finish quickly.
    monkeypatch.setattr(sar, "RETRY_WAIT_S", 0)
    monkeypatch.setattr(sar, "RETRY_TIMEOUT_S", 1)


# ---------------------------------------------------------------------------
# PolarsReadEngine: happy paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transactional", [True, False])
def test_polars_single_query(db_path, work_dir, transactional):
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id, name FROM users ORDER BY id"],
            transactional=transactional,
        ),
        work_dir,
    )
    assert len(out) == 1
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


def test_polars_multiple_queries(db_path, work_dir):
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=[
                "SELECT id FROM users ORDER BY id",
                "SELECT total FROM orders ORDER BY id",
            ],
        ),
        work_dir,
    )
    assert len(out) == 2
    df0 = pl.concat([_read_parquet(p) for p in out[0]])
    df1 = pl.concat([_read_parquet(p) for p in out[1]])
    assert df0["id"].to_list() == [1, 2, 3, 4]
    assert df1["total"].to_list() == [9.99, 14.50]


def test_polars_chunking_emits_multiple_files(db_path, work_dir):
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            cfg={const.READ_CHUNK_SIZE_KEY: 1},
        ),
        work_dir,
    )
    # 4 rows / chunk_size=1 -> at least 4 batches.
    assert len(out[0]) >= 4


def test_polars_schema_override_applies(db_path, work_dir):
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id, age FROM users ORDER BY id"],
            schema_overrides=[{"age": DataType.STRING}],
        ),
        work_dir,
    )
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df.schema["age"] == pl.Utf8


def test_polars_named_params_filter_rows(db_path, work_dir):
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=[
                "SELECT id FROM users WHERE id > :min_id ORDER BY id",
            ],
            query_params={"min_id": 2},
        ),
        work_dir,
    )
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["id"].to_list() == [3, 4]


def test_polars_empty_params_dict_is_treated_as_no_params(db_path, work_dir):
    # ``query_params={}`` -> the engine forwards ``None`` so the
    # DBAPI doesn't choke on an empty bindings payload.
    out = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            query_params={},
        ),
        work_dir,
    )
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["id"].to_list() == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# PandasReadEngine: happy paths
# ---------------------------------------------------------------------------


def test_pandas_single_query(db_path, work_dir):
    out = sar.PandasReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id, name FROM users ORDER BY id"],
        ),
        work_dir,
    )
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["name"].to_list() == ["alice", "bob", "carol", "dave"]


def test_pandas_named_params(db_path, work_dir):
    out = sar.PandasReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id FROM users WHERE age >= :cut ORDER BY id"],
            query_params={"cut": 30},
        ),
        work_dir,
    )
    df = pl.concat([_read_parquet(p) for p in out[0]])
    assert df["id"].to_list() == [1, 3, 4]


# ---------------------------------------------------------------------------
# Cross-loader parity
# ---------------------------------------------------------------------------


def test_polars_and_pandas_return_same_rows(db_path, work_dir):
    cfg_q = _cfg(
        db_path,
        queries=["SELECT id, age FROM users WHERE age > :cut ORDER BY id"],
        query_params={"cut": 28},
    )
    pol = sar.PolarsReadEngine().read(cfg_q, work_dir)
    # Rebuild cfg because the prior pol-read consumed it (same uri,
    # different sqlite engine instance per call).
    cfg_q2 = _cfg(
        db_path,
        queries=["SELECT id, age FROM users WHERE age > :cut ORDER BY id"],
        query_params={"cut": 28},
    )
    pan = sar.PandasReadEngine().read(cfg_q2, work_dir)
    df_pol = pl.concat([_read_parquet(p) for p in pol[0]])
    df_pan = pl.concat([_read_parquet(p) for p in pan[0]])
    assert df_pol["id"].to_list() == df_pan["id"].to_list()


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


def _operational(msg: str) -> OperationalError:
    return OperationalError(msg, {}, BaseException(msg))


def _programming(msg: str) -> ProgrammingError:
    return ProgrammingError(msg, {}, BaseException(msg))


def test_operational_error_retries_then_gives_up(monkeypatch, tmp_path: Path):
    attempts = {"n": 0}

    def fake_build_engine(*a, **kw):
        attempts["n"] += 1
        raise _operational("connect failed")

    monkeypatch.setattr(_sqlite_helpers, "build_engine", fake_build_engine)

    with pytest.raises(OperationalError):
        sar.PolarsReadEngine().read(
            _cfg(tmp_path / "x.db", queries=["SELECT 1"]),
            tmp_path,
        )
    assert attempts["n"] >= 2


def test_non_operational_error_does_not_retry(monkeypatch, tmp_path: Path):
    attempts = {"n": 0}

    def fake_build_engine(*a, **kw):
        attempts["n"] += 1
        raise _programming("syntax error")

    monkeypatch.setattr(_sqlite_helpers, "build_engine", fake_build_engine)

    with pytest.raises(ProgrammingError):
        sar.PolarsReadEngine().read(
            _cfg(tmp_path / "x.db", queries=["SELECT 1"]),
            tmp_path,
        )
    assert attempts["n"] == 1


def test_engine_disposed_after_failure(monkeypatch, tmp_path: Path):
    fake_engine = MagicMock()
    fake_engine.connect.side_effect = _programming("nope")

    monkeypatch.setattr(
        _sqlite_helpers, "build_engine",
        lambda *a, **kw: fake_engine,
    )
    with pytest.raises(ProgrammingError):
        sar.PolarsReadEngine().read(
            _cfg(
                tmp_path / "x.db",
                queries=["SELECT 1"],
                transactional=False,
            ),
            tmp_path,
        )
    fake_engine.dispose.assert_called()
