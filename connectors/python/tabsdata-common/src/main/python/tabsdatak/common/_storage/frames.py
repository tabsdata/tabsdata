#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from tabsdatak.common._storage.mounts import MountDeclaration


@dataclass(frozen=True)
class FrameLocator:
    uri: str
    storage_options: dict[str, str] = field(default_factory=dict, repr=False)


def locator(uri: str, declaration: MountDeclaration) -> FrameLocator:
    return FrameLocator(uri=uri, storage_options=declaration.storage_options(uri))


def scan_parquet(source: FrameLocator) -> pl.LazyFrame:
    return pl.scan_parquet(source.uri, storage_options=source.storage_options)


def sink_parquet(frame: pl.LazyFrame, destination: FrameLocator) -> None:
    frame.sink_parquet(destination.uri, storage_options=destination.storage_options)
