#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from tabsdatak.common._storage.landed import OVERFLOW_COLUMN, StoredMetadata, serve
from tabsdatak.spi import FrameMetadata, LazyFrameInput


@dataclass(frozen=True)
class Landed:
    data: pl.DataFrame
    metadata: FrameMetadata
    served: pl.DataFrame

    @property
    def schema(self) -> pl.Schema | None:
        return self.metadata.schema


class Source:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.streams: list[io.BytesIO] = []

    def __call__(self) -> io.BytesIO:
        stream = io.BytesIO(self.content)
        self.streams.append(stream)
        return stream


def land(table: LazyFrameInput, folder: Path) -> Landed:
    target = folder / "landed.parquet"
    table.frame.sink_parquet(target)
    metadata = table.metadata() if callable(table.metadata) else table.metadata
    stored = StoredMetadata.read(StoredMetadata.of(metadata or FrameMetadata()))
    served = serve(pl.scan_parquet(target), stored)
    return Landed(pl.read_parquet(target), stored, served.collect())


def strings(landed: Landed) -> pl.DataFrame:
    if OVERFLOW_COLUMN not in landed.data.columns:
        return landed.data
    assert landed.data[OVERFLOW_COLUMN].null_count() == landed.data.height
    return landed.data.drop(OVERFLOW_COLUMN)
