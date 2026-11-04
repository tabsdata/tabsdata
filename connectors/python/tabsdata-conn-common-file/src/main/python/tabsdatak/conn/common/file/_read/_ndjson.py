#
# Copyright 2026 Tabsdata Inc.
#

"""NDJSON (newline-delimited JSON: one object per line), read in blocks of
whole lines. A whole-document JSON file is not read.

- data: one String column `line`, one row per object, lossless;
- metadata: the NDJSON layout, and the fields polars decodes from each
  block's objects, folded over the whole file;
- refused: a line that is not one JSON object, named by its number;
- skipped: empty lines, as polars skips them.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator

import polars as pl
import pyarrow as pa

from tabsdatak.common._storage.failures import recorded
from tabsdatak.conn.common.file._read._blocks import (
    BLOCK_SIZE,
    EOL,
    LINE_COLUMN,
    Readable,
    blocks,
    close,
    lines,
)
from tabsdatak.conn.common.file._read._types import Fold, fields
from tabsdatak.spi import FrameMetadata, Layout, LazyFrameInput, SourceFormat

CR = "\r"


class NdjsonLineError(ValueError):
    def __init__(self, number: int, reason: str) -> None:
        super().__init__(f"Line {number} is not a JSON object: {reason}")
        self.number = number


class AutoNdjson:
    def __init__(self, *, block_size: int = BLOCK_SIZE) -> None:
        self.block_size = block_size

    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput:
        fold = Fold()
        return LazyFrameInput(
            self.data(open_, fold),
            metadata=lambda: self.metadata(fold),
        )

    def data(self, open_: Callable[[], Readable], fold: Fold) -> pl.LazyFrame:

        def batches() -> Iterator[pa.RecordBatch]:
            stream = open_()
            try:
                first = 0
                for block in blocks(stream, size=self.block_size, eol=EOL):
                    read = lines(block)
                    objects = read.filter(read.str.strip_chars() != "")
                    if objects.len():
                        fold.observe(types(first, read, objects))
                        yield from objects.to_frame().to_arrow().to_batches()
                    first += read.len()
            finally:
                close(stream)

        arrow = pa.schema({LINE_COLUMN: pa.large_string()})
        reader = pa.RecordBatchReader.from_batches(arrow, recorded(batches()))
        return pl.scan_arrow_c_stream(reader)

    @staticmethod
    def metadata(fold: Fold) -> FrameMetadata:
        return FrameMetadata(
            schema=fold.schema(),
            layout=Layout.SERIALIZED,
            format=SourceFormat.NDJSON,
            typed=False,
        )


def types(first: int, read: pl.Series, objects: pl.Series) -> dict[str, pl.DataType]:
    try:
        decoded = objects.str.json_decode(infer_schema_length=None)
    except pl.exceptions.PolarsError as error:
        raise offending(first, read, str(error)) from error
    if not isinstance(decoded.dtype, pl.Struct):
        raise offending(first, read, f"it holds {decoded.dtype}")
    if decoded.null_count():
        raise offending(first, read, "it is null")
    return fields(decoded.dtype)


def offending(first: int, read: pl.Series, reason: str) -> NdjsonLineError:
    for offset, line in enumerate(read):
        text = line.rstrip(CR)
        if not text.strip():
            continue
        try:
            value = json.loads(text, object_pairs_hook=unique)
        except ValueError as error:
            return NdjsonLineError(first + offset + 1, str(error))
        if not isinstance(value, dict):
            return NdjsonLineError(first + offset + 1, f"it holds {kind(value)}")
    return NdjsonLineError(first + 1, reason)


def kind(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "a string"
    return "an array"


def unique(pairs: list[tuple[str, object]]) -> dict:
    keys = [key for key, _ in pairs]
    repeated = sorted({key for key in keys if keys.count(key) > 1})
    if repeated:
        raise ValueError(f"the keys {repeated} repeat")
    return dict(pairs)
