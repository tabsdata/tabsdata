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

    @classmethod
    def of(cls, uri: str, declaration: MountDeclaration) -> FrameLocator:
        return cls(uri=uri, storage_options=declaration.storage_options(uri))

    def scan(self) -> pl.LazyFrame:
        return pl.scan_parquet(self.uri, storage_options=self.storage_options)

    def sink(self, frame: pl.LazyFrame) -> None:
        frame.sink_parquet(self.uri, storage_options=self.storage_options)
