#
# Copyright 2026 Tabsdata Inc.
#

"""CSV read in blocks of whole records, each parsed by polars:

- data: every column a String, as polars reads it with the source's
  options;
- metadata: the types polars infers for each block, folded over the whole
  file, parsed with the source's `decimal_comma`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any

import polars as pl
import pyarrow as pa

from tabsdata.expansions.foundation.readers import CsvCutter  # noqa: TID251
from tabsdatak.conn.common.file._read._blocks import (
    BLOCK_SIZE,
    Readable,
    close,
    records,
)
from tabsdatak.conn.common.file._read._types import Fold
from tabsdatak.spi import (
    CsvParseOptions,
    FrameMetadata,
    LazyFrameInput,
    SourceFormat,
)

FIRST_BLOCK_OPTIONS = ("has_header", "skip_rows", "skip_rows_after_header")
DECIMAL_COMMA = "decimal_comma"
TRY_PARSE_DATES = "try_parse_dates"
MISSING_IS_NULL = "missing_is_null"
QUOTE_CHAR = "quote_char"
EOL_CHAR = "eol_char"
SEPARATOR = "separator"
COMMENT_PREFIX = "comment_prefix"
DEFAULT_SEPARATOR = ","
DEFAULT_QUOTE = '"'
HEADER_LETTERS = "abcd"
DEFAULT_EOL = "\n"


class AutoCsv:
    def __init__(
        self,
        options: Mapping[str, Any] | None = None,
        *,
        block_size: int = BLOCK_SIZE,
    ) -> None:
        self.options = dict(options or {})
        self.try_parse_dates = bool(self.options.pop(TRY_PARSE_DATES, False))
        self.options.pop(MISSING_IS_NULL, None)
        self.block_size = block_size

    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput:
        fold = Fold()
        frame, columns = self.data(open_, fold)
        return LazyFrameInput(frame, metadata=lambda: self.metadata(fold, columns))

    def data(
        self,
        open_: Callable[[], Readable],
        fold: Fold,
    ) -> tuple[pl.LazyFrame, list[str]]:
        stream = open_()
        records = self.records(stream)
        first, strings = self.first(records)
        columns = strings.columns
        later = {
            **{k: v for k, v in self.options.items() if k not in FIRST_BLOCK_OPTIONS},
            "has_header": True,
            "new_columns": columns,
            "raise_if_empty": False,
        }
        header = self.header(len(columns))

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield from self.landed(fold, strings, first, self.options)
                for records_block in records:
                    block = header + records_block
                    parsed = pl.read_csv(block, infer_schema=False, **later)
                    yield from self.landed(fold, parsed, block, later)
            finally:
                close(stream)

        arrow = strings.to_arrow().schema
        reader = pa.RecordBatchReader.from_batches(arrow, batches())
        return pl.scan_arrow_c_stream(reader), columns

    def metadata(self, fold: Fold, columns: list[str]) -> FrameMetadata:
        return FrameMetadata(
            schema=fold.schema(columns),
            format=SourceFormat.CSV,
            typed=False,
            parsing=CsvParseOptions(
                decimal_comma=bool(self.options.get(DECIMAL_COMMA, False)),
            ),
        )

    # The first block holds what the first-block options skip and the header;
    # it grows until polars reads a row from it, or the file ends.
    def first(self, records: Iterator[bytes]) -> tuple[bytes, pl.DataFrame]:
        block = b""
        for record in records:
            block += record
            try:
                strings = pl.read_csv(block, infer_schema=False, **self.options)
            except pl.exceptions.NoDataError:
                continue
            if strings.height:
                return block, strings
        return block, pl.read_csv(block, infer_schema=False, **self.options)

    # ToDo(PW2): provisional, until polars can be told a block's width. polars
    # takes a file's width from its header: rows short of it get nulls,
    # wider ones fail or, with `truncate_ragged_lines`, are cut. A later
    # block is parsed behind a made-up header line of the columns' width,
    # renamed to the real names after, so its rows read as in the whole
    # file. The made-up names are a letter and a number; the letter is the
    # first of `HEADER_LETTERS` that is not the separator, the quote or the
    # comment prefix's first character, so the line splits into the width
    # and is not taken for a comment.
    def header(self, width: int) -> bytes:
        separator = self.options.get(SEPARATOR, DEFAULT_SEPARATOR)
        taken = {
            separator,
            self.options.get(QUOTE_CHAR, DEFAULT_QUOTE),
            (self.options.get(COMMENT_PREFIX) or "")[:1],
        }
        letter = next(letter for letter in HEADER_LETTERS if letter not in taken)
        eol = self.options.get(EOL_CHAR, DEFAULT_EOL)
        names = separator.join(f"{letter}{position}" for position in range(width))
        return f"{names}{eol}".encode()

    def records(self, stream: Readable) -> Iterator[bytes]:
        cutter = CsvCutter(
            self.options.get(SEPARATOR, DEFAULT_SEPARATOR),
            self.options.get(QUOTE_CHAR, DEFAULT_QUOTE),
            self.options.get(EOL_CHAR, DEFAULT_EOL),
            self.options.get(COMMENT_PREFIX),
        )
        return (block for block, _ in records(stream, cutter, size=self.block_size))

    def landed(
        self,
        fold: Fold,
        strings: pl.DataFrame,
        block: bytes,
        options: Mapping[str, Any],
    ) -> Iterator[pa.RecordBatch]:
        if strings.height:
            fold.observe(self.types(strings, block, options))
            yield from strings.to_arrow().to_batches()

    def types(
        self,
        strings: pl.DataFrame,
        block: bytes,
        options: Mapping[str, Any],
    ) -> dict[str, pl.DataType]:
        typed = pl.scan_csv(
            block,
            infer_schema_length=None,
            try_parse_dates=self.try_parse_dates,
            **options,
        ).collect_schema()
        return {
            name: dtype
            for name, dtype in typed.items()
            if strings[name].null_count() < strings.height
        }
