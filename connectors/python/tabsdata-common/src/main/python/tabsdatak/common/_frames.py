#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterator
from typing import IO, Union

import pandas as pd
import polars as pl
import pyarrow as pa


def to_arrow(frame: pl.LazyFrame) -> pa.RecordBatchReader:
    schema = pl.DataFrame(schema=frame.collect_schema()).to_arrow().schema
    return pa.RecordBatchReader.from_batches(
        schema,
        (batch for chunk in _batches(frame) for batch in chunk.to_arrow().to_batches()),
    )


def to_pandas(frame: pl.LazyFrame) -> Iterator[pd.DataFrame]:
    return (chunk.to_pandas() for chunk in _batches(frame))


def to_parquet(frame: pl.LazyFrame, sink: IO[bytes]) -> None:
    frame.sink_parquet(sink)


def to_polars(
    data: Union[
        pl.LazyFrame,
        pl.DataFrame,
        pa.RecordBatchReader,
        pa.Table,
        pd.DataFrame,
        bytes,
        IO[bytes],
    ],
) -> pl.LazyFrame:
    if isinstance(data, pl.LazyFrame):
        return data
    if isinstance(data, pl.DataFrame):
        return data.lazy()
    if isinstance(data, pa.RecordBatchReader):
        return pl.scan_arrow_c_stream(data)
    if isinstance(data, pa.Table):
        return pl.DataFrame(data).lazy()
    if isinstance(data, pd.DataFrame):
        return pl.from_pandas(data).lazy()
    if isinstance(data, bytes) or hasattr(data, "read"):
        return pl.scan_parquet(data)
    raise TypeError(
        f"Cannot make a LazyFrame from a {type(data).__name__}: expected a "
        "polars LazyFrame or DataFrame, a pyarrow RecordBatchReader or "
        "Table, a pandas DataFrame, or parquet bytes or binary stream"
    )


def _batches(frame: pl.LazyFrame) -> Iterator[pl.DataFrame]:
    return iter(frame.collect_batches(lazy=True, engine="streaming"))
