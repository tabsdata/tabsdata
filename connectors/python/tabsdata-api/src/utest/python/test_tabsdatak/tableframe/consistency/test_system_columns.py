#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME as ID,
)
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

pytestmark = pytest.mark.tableframe

ROWS = 100_000


def _frame(lf: pl.LazyFrame) -> TableFrame:
    return TableFrame._build(lf, CreationMode.MUT, idx=0)


@pytest.mark.parametrize("engine", ["in-memory", "streaming"])
def test_every_row_gets_its_own_identifier(engine):
    ids = _frame(pl.LazyFrame({"x": range(ROWS)}))._persisted().collect(engine=engine)
    assert ids[ID].n_unique() == ROWS


def test_a_stored_table_keeps_one_identifier_per_row(tmp_path):
    pl.DataFrame({"x": range(ROWS)}).write_parquet(tmp_path / "in.parquet")
    frame = _frame(pl.scan_parquet(tmp_path / "in.parquet"))
    frame._persisted().sink_parquet(tmp_path / "out.parquet")
    assert pl.read_parquet(tmp_path / "out.parquet")[ID].n_unique() == ROWS


def test_an_empty_frame_gets_an_empty_identifier_column():
    frame = _frame(pl.LazyFrame(schema={"x": pl.Int64}))._persisted().collect()
    assert frame.height == 0
    assert ID in frame.columns


ENGINES = ["in-memory", "streaming"]
VERSION = "$td.ver.version"
FILE = "@td.file"


def _versioned(data: dict, idx: int, version: str, meta=None) -> TableFrame:
    from datetime import datetime, timezone

    from tabsdatak.tableframe._engine.column.meta import add_meta_columns
    from tabsdatak.tableframe._engine.model.frame import DataVersion

    lf = pl.LazyFrame(data)
    if meta is not None:
        lf = add_meta_columns(lf, meta)
    stamp = (
        DataVersion
        .builder()
        .with_execution(f"E{version}")
        .with_transaction(f"T{version}")
        .with_version(version)
        .with_timestamp(datetime(2026, 1, 1, tzinfo=timezone.utc))
        .build()
    )
    return TableFrame._build(lf, CreationMode.MUT, idx=idx, version=stamp)


def _rows(start: int = 0) -> dict:
    keys = range(start, start + ROWS)
    return {"k": [key % 1000 for key in keys], "v": list(keys)}


def _collect(frame: TableFrame, engine: str) -> pl.DataFrame:
    return frame._lf.collect(engine=engine)


@pytest.mark.parametrize("engine", ENGINES)
def test_an_op_that_drops_the_version_restates_the_inputs(engine):
    from tabsdatak.tableframe import functions as td

    frame = _versioned(_rows(), 0, "V1")
    assert _collect(frame.select("v"), engine)[VERSION].unique().to_list() == ["V1"]
    grouped = _collect(frame.group_by("k").agg(td.col("v").sum()), engine)
    assert grouped.height == 1000
    assert grouped[VERSION].unique().to_list() == ["V1"]


@pytest.mark.parametrize("engine", ENGINES)
def test_stacked_rows_keep_their_own_version(engine):
    from tabsdatak.tableframe import functions as td

    a = _versioned(_rows(), 0, "V1")
    b = _versioned(_rows(ROWS), 1, "V2")
    stacked = td.concat([a, b])
    for frame in (stacked, stacked.select("v"), stacked.filter(td.col("v") >= 0)):
        got = _collect(frame, engine)
        assert got.height == 2 * ROWS
        wrong = got.filter((pl.col("v") < ROWS) != (pl.col(VERSION) == "V1"))
        assert wrong.height == 0


@pytest.mark.parametrize("engine", ENGINES)
def test_a_group_carries_its_latest_version(engine):
    from tabsdatak.tableframe import functions as td

    a = _versioned({"k": list(range(ROWS)), "v": [1] * ROWS}, 0, "V1")
    b = _versioned(
        {"k": list(range(ROWS // 2, ROWS + ROWS // 2)), "v": [1] * ROWS}, 1, "V2"
    )
    grouped = _collect(td.concat([a, b]).group_by("k").agg(td.col("v").sum()), engine)
    expected = (
        pl.when(pl.col("k") < ROWS // 2).then(pl.lit("V1")).otherwise(pl.lit("V2"))
    )
    assert grouped.height == ROWS + ROWS // 2
    assert grouped.filter(pl.col(VERSION) != expected).height == 0


@pytest.mark.parametrize("engine", ENGINES)
def test_a_join_keeps_the_left_version_on_every_row(engine):
    a = _versioned(_rows(), 0, "V1")
    b = _versioned({"v": list(range(ROWS // 2, ROWS + ROWS // 2))}, 1, "V2")
    joined = _collect(a.join(b, on="v", how="full"), engine)
    assert joined.height == ROWS + ROWS // 2
    assert joined[VERSION].unique().to_list() == ["V1"]


@pytest.mark.parametrize("engine", ENGINES)
def test_a_join_keeps_the_left_meta_column_only(engine):
    a = _versioned(_rows(), 0, "V1", {"file": "a.csv"})
    b = _versioned({"v": list(range(ROWS))}, 1, "V2", {"file": "b.csv"})
    joined = a.join(b, on="v")
    names = joined._persisted().collect_schema().names()
    assert [name for name in names if name.startswith("@")] == [FILE]
    assert _collect(joined, engine)[FILE].unique().to_list() == ["a.csv"]


@pytest.mark.parametrize(
    "mapping", [{"$td.id": "foo"}, {"v": "$td.id"}], ids=["away", "onto"]
)
def test_a_rename_cannot_move_a_system_column(mapping):
    frame = _versioned({"v": [1]}, 0, "V1")
    with pytest.raises(Exception, match="TF6"):
        frame.rename(mapping)


META = {"file": "a.csv"}


def _texts(rows: int) -> TableFrame:
    data = {"v": list(range(rows)), "s": [f"x{row}0" for row in range(rows)]}
    schema = {"v": pl.Int64, "s": pl.String}
    return _versioned(pl.DataFrame(data, schema=schema).to_dict(), 0, "V1", META)


def _patterns():
    from tabsdatak.tableframe import datatypes as dt
    from tabsdatak.tableframe.functions import col

    return {
        "wildcard": col("*").cast(dt.String),
        "regex": col("^.*$").cast(dt.String),
        "dtype": col(dt.String).str.replace_all("0", "Z"),
    }


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("rows", [0, 3])
@pytest.mark.parametrize("pattern", ["wildcard", "regex", "dtype"])
def test_a_pattern_selection_leaves_the_internal_columns_alone(engine, rows, pattern):
    frame = _texts(rows)
    before = _collect(frame, engine)
    after = _collect(frame.with_columns(_patterns()[pattern]), engine)
    internal = [name for name in before.columns if name.startswith("$td.")]
    assert after.select(internal).schema == before.select(internal).schema
    assert after[VERSION].to_list() == before[VERSION].to_list()
    if pattern == "dtype":
        expected = [f"x{row}0".replace("0", "Z") for row in range(rows)]
        assert after["s"].to_list() == expected
    else:
        assert after.schema["v"] == pl.String


@pytest.mark.parametrize("engine", ENGINES)
def test_a_pattern_selection_still_reaches_the_meta_columns(engine):
    from tabsdatak.tableframe import datatypes as dt
    from tabsdatak.tableframe.functions import col

    frame = _texts(2).with_columns(col(dt.String).str.to_uppercase())
    assert _collect(frame, engine)[FILE].to_list() == ["A.CSV", "A.CSV"]


@pytest.mark.parametrize("engine", ENGINES)
def test_an_internal_column_can_still_be_read_by_name(engine):
    from tabsdatak.tableframe.functions import col

    frame = _texts(2)
    read = _collect(frame.select(col(VERSION).alias("ver")), engine)
    assert read["ver"].to_list() == ["V1", "V1"]
    kept = _collect(frame.with_columns(col(VERSION)), engine)
    assert kept[VERSION].to_list() == ["V1", "V1"]


@pytest.mark.parametrize("rows", [0, 3])
def test_a_cast_cannot_name_an_internal_column(rows):
    from tabsdatak.tableframe import datatypes as dt

    frame = _texts(rows)
    with pytest.raises(Exception, match="TF6"):
        frame.cast({"$td.src": dt.String})
    every = frame.get_columns(with_meta_columns=True, with_internal_columns=True)
    with pytest.raises(Exception, match="TF6"):
        frame.cast({name: dt.String for name in every if not name.startswith("@td.")})


@pytest.mark.parametrize("rows", [0, 3])
def test_an_expression_cannot_rewrite_an_internal_column(rows):
    from tabsdatak.tableframe import datatypes as dt
    from tabsdatak.tableframe.functions import col

    frame = _texts(rows)
    with pytest.raises(Exception, match="TF6"):
        frame.with_columns(col("$td.src").cast(dt.String))
    with pytest.raises(Exception, match="TF6"):
        frame.with_columns(**{VERSION: col(VERSION).str.to_uppercase()})


def test_no_internal_column_is_a_float_or_a_struct():
    from test_tabsdatak.tableframe.support.columns import registry

    for name, column in registry().metadata().items():
        if name.startswith("$td."):
            assert not column.dtype.is_float(), name
            assert column.dtype.base_type() is not pl.Struct, name


def _carried() -> TableFrame:
    from tabsdatak.tableframe.functions import col

    data = {
        "k": [0, 3, 1, 2],
        "f": [0.0, float("nan"), 1.0, 2.0],
        "st": [{"x": 0}, {"x": 3}, {"x": 1}, {"x": 2}],
    }
    return _versioned(data, 0, "V1", META).filter(col("k") > 0)


def _by_key(frame: pl.DataFrame) -> dict:
    internal = [
        name for name in frame.columns if name.startswith("$td.") and name != ID
    ]
    return {
        row["k"]: [row[name] for name in internal]
        for row in frame.iter_rows(named=True)
    }


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("op", ["fill_nan", "drop_nans", "unnest", "sort"])
def test_frame_wide_ops_carry_each_rows_internal_columns(engine, op):
    frame = _carried()
    result = {
        "fill_nan": lambda: frame.fill_nan(0.0),
        "drop_nans": lambda: frame.drop_nans(),
        "unnest": lambda: frame.unnest(),
        "sort": lambda: frame.sort("*"),
    }[op]()
    before = _by_key(_collect(frame, engine))
    after = _by_key(_collect(result, engine))
    assert after == {key: before[key] for key in after}


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("pattern", ["*", "^.*$", r"^@td\..*$"])
def test_a_drop_by_pattern_keeps_the_internal_columns(engine, pattern):
    frame = _carried()
    before = _collect(frame, engine)
    after = _collect(frame.drop(pattern), engine)
    assert after["$td.row"].to_list() == before["$td.row"].to_list()
    assert after[VERSION].to_list() == before[VERSION].to_list()
    assert FILE not in after.columns


def test_a_drop_by_name_cannot_remove_an_internal_column():
    with pytest.raises(Exception, match="TF6"):
        _carried().drop("$td.row")


@pytest.mark.parametrize("engine", ENGINES)
def test_drop_nulls_counts_the_user_columns_only(engine):
    from tabsdatak.tableframe import functions as td

    a = _versioned(
        {"v": [None if v % 2 else v for v in range(ROWS)]}, 0, "V1", {"file": "a.csv"}
    )
    b = _versioned({"v": list(range(ROWS))}, 1, "V1")
    kept = _collect(td.concat([a, b], how="diagonal").drop_nulls(), engine)
    assert kept.height == ROWS // 2 + ROWS


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(
    ("height", "keep", "expected"),
    [
        (3, "any", 1),
        (3, "first", 1),
        (3, "last", 1),
        (3, "none", 0),
        (1, "none", 1),
        (0, "any", 0),
    ],
)
def test_unique_without_user_columns_keeps_one_row(engine, height, keep, expected):
    frame = _versioned({"v": list(range(height))}, 0, "V1", {"file": "a.csv"})
    bare = frame.drop("v")
    assert bare._user_columns() == []
    assert _collect(bare.unique(keep=keep), engine).height == expected
    assert _collect(frame.unique(subset=[], keep=keep), engine).height == expected


@pytest.mark.parametrize("engine", ENGINES)
def test_unique_without_user_columns_keeps_the_kept_row_whole(engine):
    frame = _versioned({"v": [1, 2, 3]}, 0, "V1", {"file": "a.csv"})
    first = _collect(frame.drop("v").unique(keep="first"), engine)
    last = _collect(frame.drop("v").unique(keep="last"), engine)
    assert first["$td.row"].to_list() == [0]
    assert last["$td.row"].to_list() == [2]
    assert first[FILE].to_list() == ["a.csv"]


@pytest.mark.parametrize("engine", ENGINES)
def test_fill_null_and_cast_keep_every_row_aligned(engine):
    from tabsdatak.tableframe import datatypes as dt

    data = {
        "v": list(range(ROWS)),
        "s": [None if v % 3 else str(v) for v in range(ROWS)],
    }
    frame = _versioned(data, 0, "V1", {"file": "a.csv"})
    filled = _collect(frame.fill_null("Z"), engine)
    expected = (
        pl
        .when(pl.col("v") % 3 == 0)
        .then(pl.col("v").cast(pl.String))
        .otherwise(pl.lit("Z"))
    )
    assert filled.filter(pl.col("s") != expected).height == 0
    assert filled[FILE].unique().to_list() == ["a.csv"]
    cast = _collect(frame.cast({dt.String: dt.Int64}, strict=False), engine)
    assert (
        cast.filter((pl.col("v") % 3 == 0) & (pl.col("s") != pl.col("v"))).height == 0
    )
    assert cast.schema[FILE] == pl.String
    assert cast[VERSION].unique().to_list() == ["V1"]
