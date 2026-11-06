#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import threading

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql import _spool

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.footer import column_range

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._spool import piped_parquet

pytestmark = pytest.mark.sql_common


ROWS = pl.LazyFrame({"id": range(200_000), "name": ["x" * 16] * 200_000})


def test_the_pipe_carries_the_whole_parquet():
    with piped_parquet(ROWS) as pipe:
        data = pipe.read()
    assert pl.read_parquet(io.BytesIO(data)).equals(ROWS.collect())


def test_a_sized_read_is_short_only_at_the_end():
    """A reader takes a short read for the end of the stream."""
    with piped_parquet(ROWS) as pipe:
        sizes = []
        while chunk := pipe.read(4096):
            sizes.append(len(chunk))
    assert all(size == 4096 for size in sizes[:-1])
    assert 0 < sizes[-1] <= 4096


def test_an_empty_frame_still_carries_its_schema():
    with piped_parquet(pl.LazyFrame(schema={"id": pl.Int64})) as pipe:
        data = pipe.read()
    assert pl.read_parquet(io.BytesIO(data)).columns == ["id"]


def test_a_failing_frame_fails_the_reader():
    """A failure must reach the reader as an error, never as an end of stream
    that would store a truncated file."""
    broken = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").str.to_integer())
    with pytest.raises(pl.exceptions.ComputeError):
        with piped_parquet(broken) as pipe:
            pipe.read()


def test_a_reader_leaving_early_stops_the_writer():
    before = threading.active_count()
    with piped_parquet(ROWS) as pipe:
        pipe.read(16)
    assert threading.active_count() == before


def test_a_failing_reader_stops_the_writer():
    before = threading.active_count()
    with pytest.raises(RuntimeError):
        with piped_parquet(ROWS) as pipe:
            pipe.read(16)
            raise RuntimeError("upload failed")
    assert threading.active_count() == before


def test_the_pipe_is_a_non_seekable_reader():
    with piped_parquet(ROWS) as pipe:
        assert pipe.readable()
        assert not pipe.seekable()
        assert not pipe.writable()


def test_the_footer_is_read_off_the_tail_of_the_stream():
    frame = pl.LazyFrame({"n": list(range(100_000))}, schema={"n": pl.Int64})
    with piped_parquet(frame) as pipe:
        pipe.read()
    footer = pipe.footer()
    assert footer.num_rows == 100_000
    assert column_range([footer], "n") == (0, 99_999)


def test_a_stream_that_failed_has_no_footer():
    frame = pl.LazyFrame({"n": [1]}).select(pl.col("missing"))
    with piped_parquet(frame) as pipe:
        with pytest.raises(Exception):
            pipe.read()
    assert pipe.footer() is None


def test_a_footer_larger_than_the_kept_tail_is_not_read(monkeypatch):
    """The tail is bounded: a footer it no longer holds whole is not read."""
    monkeypatch.setattr(_spool, "TAIL_BYTES", 16)
    stream = io.BytesIO()
    pl.DataFrame({"n": list(range(1_000))}).write_parquet(stream)
    pipe = _spool.ParquetPipe()
    for byte in stream.getvalue():
        pipe._keep(bytes([byte]))
    pipe._produced = True
    assert pipe._tail_size == 16
    assert pipe.footer() is None
