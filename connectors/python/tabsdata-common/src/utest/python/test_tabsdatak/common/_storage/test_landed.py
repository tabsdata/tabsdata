#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.common._storage.landed import OVERFLOW_COLUMN, mismatch, serve
from tabsdatak.spi import CsvParseOptions, FrameMetadata, SourceFormat

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

OVERFLOW_TYPE = pl.List(pl.String)


def csv_metadata(schema: dict[str, pl.DataType]) -> FrameMetadata:
    return FrameMetadata(
        schema=pl.Schema(schema),
        format=SourceFormat.CSV,
        typed=False,
        parsing=CsvParseOptions(decimal_comma=False),
    )


def landed(
    rows: dict[str, list], overflow: list[list[str | None] | None]
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {**rows, OVERFLOW_COLUMN: overflow},
        schema={**{name: pl.String for name in rows}, OVERFLOW_COLUMN: OVERFLOW_TYPE},
    )


def test_the_extras_are_read_by_position_from_the_overflow_list():
    frame = landed({"a": ["1", "3", "6"]}, [None, ["5"], ["8", "9"]])
    metadata = csv_metadata({"a": pl.Int64, "column_1": pl.Int64, "column_2": pl.Int64})
    served = serve(frame, metadata).collect()
    assert served.to_dict(as_series=False) == {
        "a": [1, 3, 6],
        "column_1": [None, 5, 8],
        "column_2": [None, None, 9],
    }


def test_the_overflow_column_never_reaches_the_served_frame():
    frame = landed({"a": ["1"]}, [None])
    served = serve(frame, csv_metadata({"a": pl.Int64})).collect()
    assert served.columns == ["a"]


def test_an_extra_is_parsed_like_any_other_column():
    frame = landed({"a": ["x"]}, [["1,5"]])
    metadata = FrameMetadata(
        schema=pl.Schema({"a": pl.String, "column_1": pl.Float64}),
        format=SourceFormat.CSV,
        typed=False,
        parsing=CsvParseOptions(decimal_comma=True),
    )
    assert serve(frame, metadata).collect()["column_1"].to_list() == [1.5]


def test_data_without_an_overflow_column_is_served_as_before():
    frame = pl.LazyFrame({"a": ["1"], "b": ["x"]})
    metadata = csv_metadata({"a": pl.Int64, "b": pl.String})
    assert serve(frame, metadata).collect().to_dict(as_series=False) == {
        "a": [1],
        "b": ["x"],
    }


def test_the_header_columns_and_the_overflow_match_the_schema():
    actual = pl.Schema({"a": pl.String, OVERFLOW_COLUMN: OVERFLOW_TYPE})
    metadata = csv_metadata({"a": pl.Int64, "column_1": pl.Int64})
    assert mismatch(actual, metadata) is None


def test_an_extra_listed_before_a_header_column_is_a_mismatch():
    actual = pl.Schema({"a": pl.String, OVERFLOW_COLUMN: OVERFLOW_TYPE})
    metadata = csv_metadata({"column_1": pl.Int64, "a": pl.Int64})
    assert mismatch(actual, metadata) is not None


def test_a_missing_column_without_an_overflow_is_still_a_mismatch():
    actual = pl.Schema({"a": pl.String})
    metadata = csv_metadata({"a": pl.Int64, "b": pl.Int64})
    assert mismatch(actual, metadata) is not None
