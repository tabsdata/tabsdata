#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    PATH_SEPARATOR,
    Stage,
)
from tabsdatak.server._tdrunner.function._handlers.landing import (
    LandedPair,
    Landing,
)
from tabsdatak.server._tdrunner.function._handlers.tables import (
    TableFile,
    TableFileError,
)
from tabsdatak.spi import LazyFrameInput, StageFileStats

logger = logging.getLogger(__name__)


class TableCountError(ValueError):
    def __init__(self, given: int, tables: int) -> None:
        super().__init__(
            f"Cannot send {given} table entries: the function has {tables} "
            f"table(s), and each needs its entry, None for no table"
        )
        self.given = given
        self.tables = tables


class TableTypeError(TypeError):
    def __init__(self, position: int, table: object) -> None:
        super().__init__(
            f"Cannot send a {type(table).__name__} for table {position}; send "
            f"LazyFrameInputs"
        )
        self.position = position
        self.table = table


class NotParquetError(ValueError):
    def __init__(self, position: int, index: int, file: Path, detail: str) -> None:
        super().__init__(
            f"Cannot stage file {index} of table {position} ({file}): it is not "
            f"a parquet file: {detail}"
        )
        self.position = position
        self.index = index
        self.file = file
        self.detail = detail


@dataclass(frozen=True)
class LocalFile:
    path: Path
    stats: StageFileStats

    @classmethod
    def check(cls, position: int, index: int, path: Path) -> LocalFile:
        if not path.is_file():
            raise FileNotFoundError(
                f"File {index} of table {position} ({path}) does not exist"
            )
        try:
            stats = TableFile.local(path).stats()
        except TableFileError as error:
            raise NotParquetError(position, index, path, str(error)) from error
        return cls(
            path,
            StageFileStats(
                name=path.name,
                rows=stats.row_count,
                bytes=stats.data_bytes,
            ),
        )


@dataclass(frozen=True)
class StagedFile:
    path: str
    stats: StageFileStats


class StageBatch:
    storage: Stage
    tables: int
    manifest: StageManifest | None
    files: list[list[StagedFile]]

    def __init__(self, storage: Stage, tables: int) -> None:
        self.storage = storage
        self.tables = tables
        self.manifest = None
        self.files = [[] for _ in range(tables)]

    @property
    def pending(self) -> list[list[StageFileStats]]:
        return [[file.stats for file in files] for files in self.files]

    def open(self) -> StageManifest:
        manifest = self.manifest
        if manifest is None:
            manifest = StageManifest.now()
            self.manifest = manifest
            logger.debug(f"Opened batch {manifest.batch}")
        return manifest

    def stage(
        self,
        tables_files: list[Iterable[LazyFrameInput] | None],
    ) -> list[list[StageFileStats]]:
        if len(tables_files) != self.tables:
            raise TableCountError(len(tables_files), self.tables)
        sent: list[list[StageFileStats]] = []
        for position, tables in enumerate(tables_files):
            stats: list[StageFileStats] = []
            for table in tables or ():
                if not isinstance(table, LazyFrameInput):
                    raise TableTypeError(position, table)
                stats.append(self.write(position, table))
            sent.append(stats)
        return sent

    def write(self, position: int, table: LazyFrameInput) -> StageFileStats:
        manifest = self.open()
        landed = Landing(self.storage.locate(manifest.folder)).land(
            position,
            len(self.files[position]),
            table.frame,
            schema=table.schema,
            metadata=table.metadata,
        )
        name = landed.data.uri.rpartition(PATH_SEPARATOR)[2]
        stats = StageFileStats(name=name, rows=landed.rows, bytes=landed.bytes)
        self.files[position].append(StagedFile(manifest.file(name), stats))
        logger.debug(f"Staged {stats.rows} row(s) of table {position} as {name}")
        return stats

    def upload(self, tables_files: list[list[Path]]) -> None:
        checked = [
            [LocalFile.check(position, index, path) for index, path in enumerate(paths)]
            for position, paths in enumerate(tables_files)
        ]
        manifest = self.open()
        while len(self.files) < len(checked):
            self.files.append([])
        for position, files in enumerate(checked):
            for file in files:
                path = manifest.file(file.path.name)
                self.storage.upload(file.path, path)
                file.path.unlink()
                self.files[position].append(StagedFile(path, file.stats))

    def publish(self) -> StageManifest:
        tables = [[file.path for file in files] for files in self.files]
        manifest = self.open().with_tables(tables)
        manifest.write(self.storage)
        self.reset()
        return manifest

    def unstage(self) -> None:
        manifest = self.manifest
        if manifest is None:
            return
        for files in self.files:
            for file in files:
                for path in LandedPair.of(file.path).names:
                    try:
                        self.storage.discard(path)
                    except Exception as error:
                        logger.warning(f"Cannot delete {path}: {error}")
        logger.info(f"Unstaged batch {manifest.batch}")
        self.reset()

    def reset(self) -> None:
        self.manifest = None
        self.files = [[] for _ in range(self.tables)]
