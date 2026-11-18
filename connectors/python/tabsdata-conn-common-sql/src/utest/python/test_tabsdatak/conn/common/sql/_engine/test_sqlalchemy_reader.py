#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak._handoff import RestartRead
from tabsdatak.conn.common.types import DataType
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_reader as sar
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadFaults
from tabsdatak.conn.common.sql._metadata import sql_metadata
from tabsdatak.error import ErrorCode, ErrorDef, TabsdataException
from tabsdatak.spi import FrameMetadata, LazyFrameInput

from test_tabsdatak.conn.common.sql import _sqlite_helpers
from test_tabsdatak.conn.common.sql._sqlite_helpers import (
    _SqliteSqlReadCfg,
    sqlite_uri,
)


class _Unreachable(TabsdataException):
    pass


class _Failed(TabsdataException):
    pass


class _Code(ErrorCode):
    UNREACHABLE = ErrorDef(_Unreachable, "Unreachable (uri={uri})")
    FAILED = ErrorDef(_Failed, "Failed (query_index={query_index})")


FAULTS = SqlReadFaults(
    unreachable=_Code.UNREACHABLE,
    failed=_Code.FAILED,
    uri="sqlite:///stub",
)


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
def engines(monkeypatch) -> list:
    built: list = []
    original = _sqlite_helpers.build_engine

    def build(*args, **kwargs):
        engine = original(*args, **kwargs)
        engine.dispose = MagicMock(wraps=engine.dispose)
        built.append(engine)
        return engine

    monkeypatch.setattr(_sqlite_helpers, "build_engine", build)
    return built


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch):
    monkeypatch.setattr(sar, "RETRY_WAIT_S", 0)
    monkeypatch.setattr(sar, "RETRY_TIMEOUT_S", 1)


def _cfg(db_path: Path, **kwargs) -> _SqliteSqlReadCfg:
    kwargs.setdefault("queries", ["SELECT 1"])
    return _SqliteSqlReadCfg(uri=sqlite_uri(db_path), **kwargs)


def _frames(slot: Iterable[LazyFrameInput]) -> list[pl.DataFrame]:
    return [table.frame.collect() for table in slot]


def _rows(slot: Iterable[LazyFrameInput]) -> pl.DataFrame:
    return pl.concat(_frames(slot))


def _read(engine: sar.SqlAlchemyReadEngine, cfg, **options) -> list[pl.DataFrame]:
    return [_rows(slot) for slot in engine.read(cfg, FAULTS, **options)]


def _operational(msg: str) -> OperationalError:
    return OperationalError(msg, {}, BaseException(msg))


def _programming(msg: str) -> ProgrammingError:
    return ProgrammingError(msg, {}, BaseException(msg))


@pytest.mark.parametrize("transactional", [True, False])
def test_polars_single_query(db_path, transactional):
    [df] = _read(
        sar.PolarsReadEngine(),
        _cfg(
            db_path,
            queries=["SELECT id, name FROM users ORDER BY id"],
            transactional=transactional,
        ),
    )
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
        {"id": 4, "name": "dave"},
    ]


def test_polars_multiple_queries(db_path):
    df0, df1 = _read(
        sar.PolarsReadEngine(),
        _cfg(
            db_path,
            queries=[
                "SELECT id FROM users ORDER BY id",
                "SELECT total FROM orders ORDER BY id",
            ],
        ),
    )
    assert df0["id"].to_list() == [1, 2, 3, 4]
    assert df1["total"].to_list() == [9.99, 14.50]


def test_polars_chunking_yields_a_table_per_chunk(db_path):
    [slot] = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            cfg={const.READ_CHUNK_SIZE_KEY: 1},
        ),
        FAULTS,
    )
    frames = _frames(slot)
    assert len(frames) >= 4
    assert pl.concat(frames)["id"].to_list() == [1, 2, 3, 4]


@pytest.mark.parametrize("engine", [sar.PolarsReadEngine, sar.PandasReadEngine])
@pytest.mark.parametrize(
    "column, override, expected",
    [
        ("age", DataType.STRING, pl.String),
        ("id", DataType.FLOAT, pl.Float64),
        ("age", DataType.BOOL, pl.Boolean),
        ("name", DataType.BYTES, pl.Binary),
    ],
)
def test_a_schema_override_applies_with_either_engine(
    db_path, engine, column, override, expected
):
    [df] = _read(
        engine(),
        _cfg(
            db_path,
            queries=["SELECT id, age, name FROM users ORDER BY id"],
            schema_overrides=[{column: override}],
        ),
    )
    assert df.schema[column] == expected
    assert df.height == 4


@pytest.mark.parametrize("engine", [sar.PolarsReadEngine, sar.PandasReadEngine])
def test_an_override_for_a_column_not_read_is_ignored(db_path, engine):
    [df] = _read(
        engine(),
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            schema_overrides=[{"missing": DataType.STRING}],
        ),
    )
    assert df.columns == ["id"]


def test_polars_named_params_filter_rows(db_path):
    [df] = _read(
        sar.PolarsReadEngine(),
        _cfg(
            db_path,
            queries=["SELECT id FROM users WHERE id > :min_id ORDER BY id"],
            initial_values={"min_id": 2},
        ),
    )
    assert df["id"].to_list() == [3, 4]


def test_polars_empty_params_dict_is_treated_as_no_params(db_path):
    [df] = _read(
        sar.PolarsReadEngine(),
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            initial_values={},
        ),
    )
    assert df["id"].to_list() == [1, 2, 3, 4]


EMPTY_QUERY = "SELECT id, name FROM users WHERE id > 100"


def _empty(engine: sar.SqlAlchemyReadEngine, db_path, **options) -> LazyFrameInput:
    [slot] = engine.read(_cfg(db_path, queries=[EMPTY_QUERY], **options), FAULTS)
    [table] = list(slot)
    return table


@pytest.mark.parametrize("engine", [sar.PolarsReadEngine, sar.PandasReadEngine])
def test_a_query_without_rows_and_untyped_columns_yields_one_unknown_table(
    db_path, engine
):
    table = _empty(engine(), db_path)
    assert table.frame.collect().shape == (0, 0)
    assert table.metadata == FrameMetadata(
        columns=sql_metadata(EMPTY_QUERY, None), known=False
    )


@pytest.mark.parametrize("engine", [sar.PolarsReadEngine, sar.PandasReadEngine])
def test_a_query_without_rows_and_typed_columns_yields_one_empty_typed_table(
    db_path, engine
):
    table = _empty(
        engine(),
        db_path,
        schema_overrides=[{"id": DataType.INT, "name": DataType.STRING}],
    )
    assert table.frame.collect().schema == pl.Schema({
        "id": pl.Int64,
        "name": pl.String,
    })
    assert table.frame.collect().height == 0
    assert table.metadata.known


@pytest.mark.parametrize("engine", [sar.PolarsReadEngine, sar.PandasReadEngine])
def test_a_query_without_rows_and_one_untyped_column_yields_an_unknown_table(
    db_path, engine
):
    table = _empty(engine(), db_path, schema_overrides=[{"id": DataType.INT}])
    assert table.frame.collect().shape == (0, 0)
    assert not table.metadata.known


def test_polars_types_a_query_without_rows_as_its_description_says(
    db_path, monkeypatch
):
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import described

    monkeypatch.setattr(
        described,
        "dtypes",
        lambda *_: {"id": pl.Int32(), "name": pl.String()},
    )
    table = _empty(sar.PolarsReadEngine(), db_path)
    assert table.frame.collect_schema() == pl.Schema({
        "id": pl.Int32,
        "name": pl.String,
    })
    assert table.metadata.known


def test_a_dropped_metadata_still_tells_a_table_is_unknown(db_path):
    [slot] = sar.PolarsReadEngine().read(
        _cfg(db_path, queries=[EMPTY_QUERY]), FAULTS, drop_metadata=True
    )
    assert [table.metadata for table in slot] == [FrameMetadata(known=False)]


def test_a_loader_yielding_nothing_yields_one_unknown_table(db_path):
    def nothing(**_) -> Iterator[pl.DataFrame]:
        yield from ()

    reading = sar.Reading(
        _cfg(db_path, queries=[EMPTY_QUERY]), nothing, FAULTS, drop_metadata=True
    )
    [table] = list(reading.slot(0))
    assert table.frame.collect().shape == (0, 0)
    assert table.metadata == FrameMetadata(known=False)


def test_pandas_single_query(db_path):
    [df] = _read(
        sar.PandasReadEngine(),
        _cfg(db_path, queries=["SELECT id, name FROM users ORDER BY id"]),
    )
    assert df["name"].to_list() == ["alice", "bob", "carol", "dave"]


def test_pandas_named_params(db_path):
    [df] = _read(
        sar.PandasReadEngine(),
        _cfg(
            db_path,
            queries=["SELECT id FROM users WHERE age >= :cut ORDER BY id"],
            initial_values={"cut": 30},
        ),
    )
    assert df["id"].to_list() == [1, 3, 4]


def test_polars_and_pandas_return_same_rows(db_path):
    query = "SELECT id, age FROM users WHERE age > :cut ORDER BY id"
    [pol] = _read(
        sar.PolarsReadEngine(),
        _cfg(db_path, queries=[query], initial_values={"cut": 28}),
    )
    [pan] = _read(
        sar.PandasReadEngine(),
        _cfg(db_path, queries=[query], initial_values={"cut": 28}),
    )
    assert pol["id"].to_list() == pan["id"].to_list()


def test_every_table_carries_its_query_and_binds(db_path):
    queries = [
        "SELECT id FROM users WHERE id > :min_id ORDER BY id",
        "SELECT id FROM orders WHERE id > :min_id ORDER BY id",
    ]
    slots = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=queries,
            initial_values={"min_id": 0},
            cfg={const.READ_CHUNK_SIZE_KEY: 1},
        ),
        FAULTS,
    )
    for query, slot in zip(queries, slots):
        expected = FrameMetadata(columns=sql_metadata(query, {"min_id": 0}))
        metadata = [table.metadata for table in slot]
        assert len(metadata) > 1
        assert all(each == expected for each in metadata)


def test_dropped_metadata_leaves_tables_without_metadata(db_path):
    [slot] = sar.PolarsReadEngine().read(
        _cfg(db_path, queries=["SELECT id FROM users"]),
        FAULTS,
        drop_metadata=True,
    )
    assert [table.metadata for table in slot] == [None]


def test_reading_starts_only_when_iterated(db_path, engines):
    sar.PolarsReadEngine().read(
        _cfg(db_path, queries=["SELECT id FROM users", "SELECT id FROM orders"]),
        FAULTS,
    )
    assert engines == []


def test_every_query_reads_through_one_engine_disposed_after_the_last(
    db_path,
    engines,
):
    first, second = sar.PolarsReadEngine().read(
        _cfg(db_path, queries=["SELECT id FROM users", "SELECT id FROM orders"]),
        FAULTS,
    )
    _frames(first)
    [engine] = engines
    engine.dispose.assert_not_called()
    _frames(second)
    assert engines == [engine]
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("transactional", [True, False])
def test_an_abandoned_read_disposes_the_engine(db_path, engines, transactional):
    [slot] = sar.PolarsReadEngine().read(
        _cfg(
            db_path,
            queries=["SELECT id FROM users ORDER BY id"],
            transactional=transactional,
            cfg={const.READ_CHUNK_SIZE_KEY: 1},
        ),
        FAULTS,
    )
    tables = iter(slot)
    next(tables)
    tables.close()
    [engine] = engines
    engine.dispose.assert_called_once()


def test_an_unreachable_database_asks_for_a_restart(monkeypatch, tmp_path):
    def unreachable(*args, **kwargs):
        raise _operational("connect failed")

    monkeypatch.setattr(_sqlite_helpers, "build_engine", unreachable)
    [slot] = sar.PolarsReadEngine().read(_cfg(tmp_path / "x.db"), FAULTS)
    with pytest.raises(RestartRead) as raised:
        _frames(slot)
    assert isinstance(raised.value.cause, _Unreachable)
    assert "uri=sqlite:///stub" in str(raised.value.cause)
    assert isinstance(raised.value.__cause__, OperationalError)
    assert (raised.value.wait, raised.value.within) == (0, 1)


def test_a_connection_lost_after_some_tables_asks_for_a_restart(db_path, engines):
    def loader(**options) -> Iterator[pl.DataFrame]:
        yield pl.DataFrame({"id": [1]})
        raise _operational("connection lost")

    class Broken(sar.SqlAlchemyReadEngine):
        def loader(self):
            return loader

    [slot] = Broken().read(_cfg(db_path), FAULTS)
    tables = iter(slot)
    assert next(tables).frame.collect()["id"].to_list() == [1]
    with pytest.raises(RestartRead):
        next(tables)
    [engine] = engines
    engine.dispose.assert_called_once()


def test_a_failed_query_reports_its_index(db_path, engines):
    def loader(*, query: str, **options) -> Iterator[pl.DataFrame]:
        if query == "bad":
            raise _programming("syntax error")
        yield pl.DataFrame({"id": [1]})

    class Failing(sar.SqlAlchemyReadEngine):
        def loader(self):
            return loader

    first, second = Failing().read(_cfg(db_path, queries=["good", "bad"]), FAULTS)
    _frames(first)
    with pytest.raises(_Failed) as raised:
        _frames(second)
    assert "query_index=1" in str(raised.value)
    [engine] = engines
    engine.dispose.assert_called_once()


def test_a_failure_to_connect_disposes_the_engine(monkeypatch, tmp_path):
    fake_engine = MagicMock()
    fake_engine.connect.side_effect = _programming("nope")
    monkeypatch.setattr(
        _sqlite_helpers,
        "build_engine",
        lambda *a, **kw: fake_engine,
    )
    [slot] = sar.PolarsReadEngine().read(
        _cfg(tmp_path / "x.db", transactional=False),
        FAULTS,
    )
    with pytest.raises(_Failed):
        _frames(slot)
    fake_engine.dispose.assert_called_once()
