#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

# noinspection PyProtectedMember
from tabsdatak.common._frames import to_arrow, to_pandas, to_parquet, to_polars

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

TABLE = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["a", None, "c"],
        "score": [1.5, 2.5, None],
    },
)


class _Unseekable(io.RawIOBase):
    def __init__(self) -> None:
        self.received = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        self.received += b
        return len(b)


def test_to_arrow_streams_the_table_as_record_batches():
    reader = to_arrow(TABLE.lazy())

    assert isinstance(reader, pa.RecordBatchReader)
    assert_frame_equal(pl.from_arrow(reader.read_all()), TABLE)


def test_to_arrow_keeps_the_schema_of_an_empty_table():
    reader = to_arrow(TABLE.clear().lazy())

    assert reader.schema.names == ["id", "name", "score"]
    assert reader.read_all().num_rows == 0


def test_to_pandas_streams_the_table_as_dataframes():
    chunks = list(to_pandas(TABLE.lazy()))

    assert chunks
    assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)
    assert_frame_equal(pl.from_pandas(pd.concat(chunks, ignore_index=True)), TABLE)


def test_to_parquet_streams_into_an_unseekable_sink():
    sink = _Unseekable()

    to_parquet(TABLE.lazy(), sink)

    assert_frame_equal(pl.read_parquet(bytes(sink.received)), TABLE)


@pytest.mark.parametrize(
    "data",
    [
        TABLE.lazy(),
        TABLE,
        TABLE.to_arrow(),
        TABLE.to_arrow().to_reader(),
        TABLE.to_pandas(),
    ],
    ids=["lazyframe", "dataframe", "arrow-table", "arrow-reader", "pandas"],
)
def test_to_polars_returns_a_lazyframe_of_the_table(data):
    frame = to_polars(data)

    assert isinstance(frame, pl.LazyFrame)
    assert_frame_equal(frame.collect(), TABLE)


def test_to_polars_reads_parquet_bytes_and_streams():
    buffer = io.BytesIO()
    TABLE.write_parquet(buffer)

    assert_frame_equal(to_polars(buffer.getvalue()).collect(), TABLE)
    buffer.seek(0)
    assert_frame_equal(to_polars(buffer).collect(), TABLE)


def test_to_polars_round_trips_every_format():
    frame = TABLE.lazy()
    sink = io.BytesIO()
    to_parquet(frame, sink)

    assert_frame_equal(to_polars(to_arrow(frame)).collect(), TABLE)
    assert_frame_equal(
        to_polars(pd.concat(to_pandas(frame), ignore_index=True)).collect(),
        TABLE,
    )
    assert_frame_equal(to_polars(sink.getvalue()).collect(), TABLE)


def test_to_polars_refuses_an_unsupported_format():
    with pytest.raises(TypeError, match="Cannot make a LazyFrame from a list"):
        to_polars([1, 2, 3])
