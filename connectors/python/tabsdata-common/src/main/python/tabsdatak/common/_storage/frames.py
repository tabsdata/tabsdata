#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from tabsdatak.common._storage.mounts import MountDeclaration


@dataclass(frozen=True)
class FrameTarget:
    uri: str
    storage_options: dict[str, str] = field(default_factory=dict)


def target(uri: str, declaration: MountDeclaration) -> FrameTarget:
    return FrameTarget(uri=uri, storage_options=declaration.storage_options(uri))


def scan_parquet(source: FrameTarget) -> pl.LazyFrame:
    return pl.scan_parquet(source.uri, storage_options=source.storage_options)


def sink_parquet(frame: pl.LazyFrame, destination: FrameTarget) -> None:
    frame.sink_parquet(destination.uri, storage_options=destination.storage_options)
