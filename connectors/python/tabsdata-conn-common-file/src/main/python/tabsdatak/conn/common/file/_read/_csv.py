#
# Copyright 2026 Tabsdata Inc.
#

"""CSV read in blocks of whole records, each parsed by polars:

- data: every column a String, as polars reads it with the source's
  options; the fields of rows wider than the header in one list column;
- metadata: the types polars infers for each block, folded over the whole
  file, parsed with the source's `decimal_comma`.
"""

from __future__ import annotations

import codecs
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa

from tabsdata.expansions.foundation.readers import CsvCutter  # noqa: TID251
from tabsdatak.common._storage.failures import recorded
from tabsdatak.common._storage.landed import OVERFLOW_COLUMN
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

HAS_HEADER = "has_header"
SKIP_LINES = "skip_lines"
SKIP_ROWS = "skip_rows"
SKIP_ROWS_AFTER_HEADER = "skip_rows_after_header"
FIRST_BLOCK_OPTIONS = (HAS_HEADER, SKIP_LINES, SKIP_ROWS, SKIP_ROWS_AFTER_HEADER)
DECIMAL_COMMA = "decimal_comma"
TRY_PARSE_DATES = "try_parse_dates"
MISSING_IS_NULL = "missing_is_null"
TRUNCATE_RAGGED_LINES = "truncate_ragged_lines"
ENCODING = "encoding"
NATIVE_ENCODINGS = ("utf8", "utf8-lossy")
QUOTE_CHAR = "quote_char"
EOL_CHAR = "eol_char"
SEPARATOR = "separator"
COMMENT_PREFIX = "comment_prefix"
DEFAULT_SEPARATOR = ","
DEFAULT_QUOTE = '"'
HEADER_LETTERS = "abcd"
DEFAULT_EOL = "\n"
CR = b"\r"
EXTRA_PREFIX = "column_"
OVERFLOW_TYPE = pl.List(pl.String)


@dataclass
class Overflow:
    width: int = 0


@dataclass(frozen=True)
class Block:
    data: bytes
    width: int
    skipped: int


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
        self.truncate = bool(self.options.get(TRUNCATE_RAGGED_LINES, False))
        self.transcoded = self.transcoding()
        self.later = {
            key: value
            for key, value in self.options.items()
            if key not in FIRST_BLOCK_OPTIONS
        }
        self.block_size = block_size

    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput:
        fold = Fold()
        overflow = Overflow()
        frame, names = self.data(open_, fold, overflow)
        return LazyFrameInput(
            frame, metadata=lambda: self.metadata(fold, names, overflow)
        )

    def data(
        self,
        open_: Callable[[], Readable],
        fold: Fold,
        overflow: Overflow,
    ) -> tuple[pl.LazyFrame, list[str]]:
        stream = open_()
        pieces = self.records(stream)
        names, first, parsed = self.first(pieces)
        landed = self.landed(first, parsed, names, fold, overflow)
        schema = landed.to_arrow().schema

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield from batches_of(landed)
                for data, widest in pieces:
                    block = Block(data, self.width(names, widest), 0)
                    parsed = self.parsed(block)
                    yield from batches_of(
                        self.landed(block, parsed, names, fold, overflow)
                    )
            finally:
                close(stream)

        reader = pa.RecordBatchReader.from_batches(schema, recorded(batches()))
        return pl.scan_arrow_c_stream(reader), names

    def metadata(
        self,
        fold: Fold,
        names: list[str],
        overflow: Overflow,
    ) -> FrameMetadata:
        columns = names + extra_names(names, overflow.width)
        return FrameMetadata(
            schema=fold.schema(columns),
            format=SourceFormat.CSV,
            typed=False,
            parsing=CsvParseOptions(
                decimal_comma=bool(self.options.get(DECIMAL_COMMA, False)),
            ),
        )

    def first(
        self,
        pieces: Iterator[tuple[bytes, int]],
    ) -> tuple[list[str], Block, pl.DataFrame]:
        data = b""
        widest = 0
        names: list[str] | None = None
        for piece, wide in pieces:
            data += piece
            widest = max(widest, wide)
            names = self.names(data)
            if names is None:
                continue
            block = Block(self.body(data), self.width(names, widest), self.skipped())
            parsed = self.parsed(block)
            if parsed.height:
                return names, block, parsed
        if names is None:
            names = pl.read_csv(data, infer_schema=False, **self.options).columns
        block = Block(self.body(data), self.width(names, widest), self.skipped())
        return names, block, self.parsed(block)

    def names(self, block: bytes) -> list[str] | None:
        try:
            return pl.read_csv(
                block, infer_schema=False, n_rows=0, **self.options
            ).columns
        except pl.exceptions.NoDataError:
            return None

    def width(self, names: list[str], widest: int) -> int:
        return len(names) if self.truncate else max(len(names), widest)

    def body(self, block: bytes) -> bytes:
        eol = self.options.get(EOL_CHAR, DEFAULT_EOL).encode()
        for _ in range(self.options.get(SKIP_LINES, 0)):
            end = block.find(eol)
            block = b"" if end < 0 else block[end + len(eol) :]
        if self.options.get(HAS_HEADER, True):
            block = block.lstrip(eol + CR)
        return block

    def skipped(self) -> int:
        return (
            self.options.get(SKIP_ROWS, 0)
            + (1 if self.options.get(HAS_HEADER, True) else 0)
            + self.options.get(SKIP_ROWS_AFTER_HEADER, 0)
        )

    def parsed(self, block: Block) -> pl.DataFrame:
        return pl.read_csv(
            self.header(block.width) + block.data,
            infer_schema=False,
            **self.block_options(block.skipped),
        )

    def block_options(self, skipped: int) -> dict[str, Any]:
        return {
            **self.later,
            HAS_HEADER: True,
            SKIP_ROWS_AFTER_HEADER: skipped,
            "raise_if_empty": False,
        }

    # polars takes a block's width only from a header line, so each block is
    # parsed behind a made-up one as wide as its widest record. Its letter
    # avoids the separator, the quote and the comment prefix.
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

    def records(self, stream: Readable) -> Iterator[tuple[bytes, int]]:
        cutter = CsvCutter(
            self.options.get(SEPARATOR, DEFAULT_SEPARATOR),
            self.options.get(QUOTE_CHAR, DEFAULT_QUOTE),
            self.options.get(EOL_CHAR, DEFAULT_EOL),
            self.options.get(COMMENT_PREFIX),
        )
        found = records(stream, cutter, size=self.block_size)
        if self.transcoded is None:
            return found
        return (
            (block.decode(self.transcoded).encode(), widest) for block, widest in found
        )

    def transcoding(self) -> str | None:
        encoding = self.options.get(ENCODING, NATIVE_ENCODINGS[0])
        if encoding in NATIVE_ENCODINGS:
            return None
        codecs.lookup(encoding)
        marks = [
            self.options.get(SEPARATOR, DEFAULT_SEPARATOR),
            self.options.get(QUOTE_CHAR, DEFAULT_QUOTE) or "",
            self.options.get(EOL_CHAR, DEFAULT_EOL),
            self.options.get(COMMENT_PREFIX) or "",
        ]
        if any(mark.encode(encoding) != mark.encode() for mark in marks):
            raise ValueError(
                f"The CSV encoding '{encoding}' is not read: its separator, quote, "
                "end of line or comment prefix are not the bytes they are in UTF-8"
            )
        self.options[ENCODING] = NATIVE_ENCODINGS[0]
        return encoding

    def landed(
        self,
        block: Block,
        parsed: pl.DataFrame,
        names: list[str],
        fold: Fold,
        overflow: Overflow,
    ) -> pl.DataFrame:
        made = parsed.columns
        header = made[: len(names)]
        extras = made[len(names) :]
        if parsed.height:
            fold.observe(self.types(block, parsed, names, overflow))
        return parsed.select(
            *(pl.col(old).alias(new) for old, new in zip(header, names, strict=True)),
            (
                pl.concat_list(extras)
                if extras
                else pl.repeat(None, pl.len(), dtype=OVERFLOW_TYPE)
            )
            .cast(OVERFLOW_TYPE)
            .alias(OVERFLOW_COLUMN),
        )

    def types(
        self,
        block: Block,
        parsed: pl.DataFrame,
        names: list[str],
        overflow: Overflow,
    ) -> dict[str, pl.DataType]:
        typed = pl.scan_csv(
            self.header(block.width) + block.data,
            infer_schema_length=None,
            try_parse_dates=self.try_parse_dates,
            **self.block_options(block.skipped),
        ).collect_schema()
        found: dict[str, pl.DataType] = {}
        extra = extra_names(names, block.width)
        for position, (made, dtype) in enumerate(typed.items()):
            if parsed[made].null_count() == parsed.height:
                continue
            if position < len(names):
                found[names[position]] = dtype
            else:
                overflow.width = max(overflow.width, position + 1)
                found[extra[position - len(names)]] = dtype
        return found


def extra_names(names: list[str], width: int) -> list[str]:
    taken = set(names)
    extras = []
    number = len(names)
    while len(extras) < width - len(names):
        name = f"{EXTRA_PREFIX}{number}"
        number += 1
        if name not in taken:
            extras.append(name)
    return extras


def batches_of(frame: pl.DataFrame) -> Iterator[pa.RecordBatch]:
    if frame.height:
        yield from frame.to_arrow().to_batches()
