#
# Copyright 2026 Tabsdata Inc.
#

"""A local stand-in for the function stage a stream source plugin stages to,
shared by the stream plugins' tests: the tables a plugin sends with
``StreamSrcPluginCtx.stage`` land as parquet files under a folder, and each
publish hands back their paths, one list per output slot, so a test reads
what a batch holds the way the batch run would.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from tabsdatak.spi import LazyFrameInput, StageFileStats, StreamStateSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalStagedFile:
    path: Path
    stats: StageFileStats


@dataclass(frozen=True)
class LocalBatch:
    tables: list[list[Path]]
    state: StreamStateSpec


class LocalStage:
    def __init__(self, folder: Path, tables: int = 0) -> None:
        self.folder = folder
        self.files: list[list[LocalStagedFile]] = [[] for _ in range(tables)]
        self.written = 0
        self.published: list[LocalBatch] = []

    @property
    def pending(self) -> list[list[StageFileStats]]:
        return [[file.stats for file in files] for files in self.files]

    def stage(
        self,
        tables_files: list[Iterable[LazyFrameInput] | None] | None = None,
        new_state: StreamStateSpec | None = None,
    ) -> list[list[StageFileStats]] | None:
        sent = None if tables_files is None else self.send(tables_files)
        if new_state is not None:
            self.published.append(LocalBatch(self.publish(), new_state))
        return sent

    def send(
        self,
        tables_files: list[Iterable[LazyFrameInput] | None],
    ) -> list[list[StageFileStats]]:
        if not self.files:
            self.files = [[] for _ in tables_files]
        if len(tables_files) != len(self.files):
            raise ValueError(
                f"Cannot send {len(tables_files)} table entries to a stage of "
                f"{len(self.files)} table(s)"
            )
        self.folder.mkdir(parents=True, exist_ok=True)
        sent: list[list[StageFileStats]] = []
        for position, tables in enumerate(tables_files):
            stats: list[StageFileStats] = []
            for table in tables or ():
                path = self.folder / f"{position}.{self.written}.parquet"
                self.written += 1
                table.frame.sink_parquet(path)
                file = LocalStagedFile(
                    path,
                    StageFileStats(
                        name=path.name,
                        rows=pq.read_metadata(path).num_rows,
                        bytes=path.stat().st_size,
                    ),
                )
                self.files[position].append(file)
                stats.append(file.stats)
            sent.append(stats)
        return sent

    def publish(self) -> list[list[Path]]:
        manifest = [[file.path for file in files] for files in self.files]
        self.files = [[] for _ in self.files]
        return manifest

    def find(self, stats: StageFileStats) -> LocalStagedFile:
        for files in self.files:
            for file in files:
                if file.stats.name == stats.name:
                    return file
        raise KeyError(f"{stats.name} is not pending")

    def scan(self, stats: StageFileStats) -> pl.LazyFrame:
        return pl.scan_parquet(self.find(stats).path)

    def unstage(self, files: Iterable[StageFileStats] | None = None) -> None:
        if files is None:
            dropped = [file for staged in self.files for file in staged]
        else:
            dropped = [self.find(stats) for stats in files]
        for staged in self.files:
            staged[:] = [file for file in staged if file not in dropped]
        for file in dropped:
            try:
                file.path.unlink(missing_ok=True)
            except OSError as error:
                logger.warning(f"Cannot delete {file.path}: {error}")
