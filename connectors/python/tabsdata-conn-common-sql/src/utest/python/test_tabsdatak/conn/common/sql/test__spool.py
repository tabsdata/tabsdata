#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import threading

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._spool import piped_parquet, spooled_parquet

pytestmark = pytest.mark.sql_common


def test_the_frame_is_written_under_the_given_name():
    frame = pl.LazyFrame({"id": [1, 2]})
    with spooled_parquet(frame, "batch.parquet") as path:
        assert path.name == "batch.parquet"
        assert pl.read_parquet(path).equals(frame.collect())


def test_the_file_is_removed_on_the_way_out():
    with spooled_parquet(pl.LazyFrame({"id": [1]}), "batch.parquet") as path:
        assert path.is_file()
    assert not path.exists()


def test_the_file_is_removed_when_the_body_fails():
    with pytest.raises(RuntimeError):
        with spooled_parquet(pl.LazyFrame({"id": [1]}), "batch.parquet") as path:
            raise RuntimeError("upload failed")
    assert not path.exists()


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
