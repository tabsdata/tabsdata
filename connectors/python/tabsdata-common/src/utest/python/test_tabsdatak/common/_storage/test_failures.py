#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import polars as pl
import pyarrow as pa
import pytest

from tabsdatak.common._storage.failures import (
    RecordedStream,
    collected,
    forget,
    record,
    recorded,
    taken,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

SCHEMA = pa.schema({"a": pa.int64()})


class SourceError(ValueError):
    pass


def streamed(items) -> pl.LazyFrame:
    return pl.scan_arrow_c_stream(pa.RecordBatchReader.from_batches(SCHEMA, items))


def broken():
    yield pa.record_batch({"a": [1]}, schema=SCHEMA)
    raise SourceError("the source broke")


def test_a_recorded_error_replaces_the_polars_error_that_wraps_it():
    with pytest.raises(SourceError, match="the source broke") as raised:
        collected(streamed(recorded(broken())))
    assert isinstance(raised.value.__cause__, pl.exceptions.PolarsError)


def test_without_a_record_the_polars_error_is_raised():
    with pytest.raises(pl.exceptions.PolarsError, match="the source broke"):
        collected(streamed(broken()))


def test_a_record_left_by_an_earlier_collect_is_not_reused():
    record(SourceError("stale"))
    with pytest.raises(pl.exceptions.PolarsError, match="the source broke"):
        collected(streamed(broken()))


def test_a_successful_collect_returns_the_frame():
    def batches():
        yield pa.record_batch({"a": [1, 2]}, schema=SCHEMA)

    assert collected(streamed(recorded(batches())))["a"].to_list() == [1, 2]


def test_a_recorded_stream_records_what_its_read_raises():
    class Broken(io.BytesIO):
        def read(self, size=-1):
            raise SourceError("unreadable")

    forget()
    with pytest.raises(SourceError):
        RecordedStream(Broken()).read(4)
    assert isinstance(taken(), SourceError)
    assert taken() is None


def test_a_recorded_stream_passes_everything_else_through():
    source = io.BytesIO(b"abc")
    stream = RecordedStream(source)
    assert stream.read(2) == b"ab"
    assert stream.closed is False
    stream.close()
    assert source.closed
