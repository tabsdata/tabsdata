#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import shutil
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from tabsdatak.server._tdrunner.function._commons.document.request import Info
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import Conn, Dest, DestContext, DestPlugin, SrcPlugin


class HandoffError(Exception):
    pass


class MissingOutputError(HandoffError):
    def __init__(self, slot: int, name: str) -> None:
        super().__init__(f"Output slot {slot} ({name!r}) has no data")
        self.slot = slot
        self.name = name


class OutputCopyError(HandoffError):
    def __init__(self, slot: str, path: str, detail: str) -> None:
        super().__init__(f"Output {slot} at {path} could not be copied: {detail}")
        self.slot = slot
        self.path = path
        self.detail = detail


class UnsupportedPluginError(HandoffError):
    def __init__(self, plugin: object) -> None:
        super().__init__(f"No handoff exists yet for {type(plugin).__name__}")
        self.plugin = plugin


class Handoff(ABC):
    @staticmethod
    def of(plugin: SrcPlugin | DestPlugin, base_folder: Path) -> DestinationHandoff:
        if isinstance(plugin, DestPlugin):
            return FileDestinationHandoff(base_folder=base_folder)
        raise UnsupportedPluginError(plugin)


class DestinationHandoff(Handoff):
    @abstractmethod
    def seed(self, info: Info, manifest: OutputManifest) -> list[Path]: ...

    @abstractmethod
    def grow(
        self,
        plugin: DestPlugin,
        context: DestContext,
        connection: Conn,
        destination: Dest,
        tables: list[Path],
    ) -> None: ...

    @abstractmethod
    def reap(self) -> None: ...


@dataclass(kw_only=True)
class FileDestinationHandoff(DestinationHandoff):
    source: ColumnPolicy = ColumnPolicy(Zone.LAND)
    target: ColumnPolicy = ColumnPolicy(Zone.EGRESS)
    base_folder: Path
    tables_folder: Path | None = field(default=None, init=False)

    def seed(self, info: Info, manifest: OutputManifest) -> list[Path]:
        self.base_folder.mkdir(parents=True, exist_ok=True)
        self.tables_folder = Path(tempfile.mkdtemp(dir=self.base_folder))
        tables: list[Path] = []
        plans: list[pl.LazyFrame] = []
        for slot, entry in enumerate(manifest.outputs):
            if entry.uri is None:
                raise MissingOutputError(slot, entry.name)
            source = TableFile.stored(info.landing_file(slot))
            table = self.tables_folder / f"{slot}.parquet"
            try:
                frame = self.source.scan(source)
                plans.append(self.target.sink(frame, TableFile.local(table)))
            except Exception as error:
                raise OutputCopyError(str(slot), source.uri, str(error)) from error
            tables.append(table)
        try:
            pl.collect_all(plans, engine="streaming")
        except Exception as error:
            raise OutputCopyError(
                ", ".join(str(slot) for slot in range(len(tables))),
                info.function_landing.uri,
                str(error),
            ) from error
        return tables

    def grow(
        self,
        plugin: DestPlugin,
        context: DestContext,
        connection: Conn,
        destination: Dest,
        tables: list[Path],
    ) -> None:
        plugin.write_out(context, connection, destination, tables)

    def reap(self) -> None:
        if self.tables_folder is not None:
            shutil.rmtree(self.tables_folder, ignore_errors=True)
            self.tables_folder = None
