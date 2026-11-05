#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import shutil
import tempfile
import time
from abc import ABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, overload
from urllib.parse import urlparse
from urllib.request import url2pathname

import polars as pl

from tabsdatak._handoff import DestinationContract, RestartRead, SourceContract
from tabsdatak.server._tdrunner.function._commons.document.message.request import Info
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.landing import (
    LandedFile,
    Landing,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import (
    Conn,
    Dest,
    DestContext,
    DestPlugin,
    FrameMetadata,
    LazyFrameInput,
    Src,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
)
from tabsdatak.tableframe import TableFrame

logger = logging.getLogger(__name__)


class HandoffError(Exception):
    pass


class UnexpectedTableError(HandoffError):
    def __init__(
        self,
        plugin: SrcPlugin,
        position: int,
        table: object,
        expected: str,
    ) -> None:
        super().__init__(
            f"{type(plugin).__name__}.read_in returned a "
            f"{type(table).__name__} for input {position}; it returns "
            f"{expected}",
        )
        self.plugin = plugin
        self.position = position
        self.table = table


class InputCountError(HandoffError):
    def __init__(
        self,
        returned: int,
        expected: int,
    ) -> None:
        super().__init__(
            f"The source plugin returned {returned} input slot(s); the source "
            f"declares {expected}",
        )
        self.returned = returned
        self.expected = expected


class MissingOutputError(HandoffError):
    def __init__(
        self,
        slot: int,
        name: str,
    ) -> None:
        super().__init__(f"Output slot {slot} ({name!r}) has no data")
        self.slot = slot
        self.name = name


class OutputReadError(HandoffError):
    def __init__(
        self,
        slot: int,
        path: str,
        detail: str,
    ) -> None:
        super().__init__(f"Output {slot} at {path} could not be read: {detail}")
        self.slot = slot
        self.path = path
        self.detail = detail


class OutputCopyError(HandoffError):
    def __init__(
        self,
        slot: str,
        path: str,
        detail: str,
    ) -> None:
        super().__init__(f"Output {slot} at {path} could not be copied: {detail}")
        self.slot = slot
        self.path = path
        self.detail = detail


class UnsupportedPluginError(HandoffError):
    def __init__(self, plugin: object) -> None:
        super().__init__(
            f"No handoff exists for {type(plugin).__name__}: it is neither a "
            "SrcPlugin nor a DestPlugin",
        )
        self.plugin = plugin


class Handoff(ABC):
    @overload
    @staticmethod
    def of(plugin: SrcPlugin, base_folder: Path) -> SourceHandoff: ...

    @overload
    @staticmethod
    def of(plugin: DestPlugin, base_folder: Path) -> DestinationHandoff: ...

    @staticmethod
    def of(
        plugin: SrcPlugin | DestPlugin,
        base_folder: Path,
    ) -> SourceHandoff | DestinationHandoff:
        if isinstance(plugin, SrcPlugin):
            return SourceHandoff(legacy=not SourceContract(type(plugin)).frames())
        if isinstance(plugin, DestPlugin):
            return DestinationHandoff(
                base_folder=base_folder,
                legacy=not DestinationContract(type(plugin)).frames(),
            )
        raise UnsupportedPluginError(plugin)


@dataclass
class Slot:
    name: str
    position: int


@dataclass
class LandingSlot(Slot):
    landing: Landing
    landed: list[LandedFile] = field(default_factory=list)
    count: int = 0

    def land(self, table: LazyFrameInput) -> None:
        index = self.count
        self.count += 1
        self.landed.append(
            self.landing.land(
                self.position,
                index,
                table,
            ),
        )

    def void(self) -> int:
        voided = len(self.landed)
        self.landed = []
        return voided


@dataclass(kw_only=True)
class SourceHandoff(Handoff):
    legacy: bool = False
    slots: list[LandingSlot] = field(default_factory=list, init=False)
    variable: bool = field(default=False, init=False)

    def seed(
        self,
        info: Info,
        names: list[str],
        *,
        variable: bool = False,
    ) -> list[LandingSlot]:
        self.variable = variable
        landing = Landing(info.function_landing)
        self.slots = [
            LandingSlot(
                name,
                position,
                landing,
            )
            for position, name in enumerate(names)
        ]
        return self.slots

    def grow(
        self,
        plugin: SrcPlugin,
        context: SrcPluginCtx,
        connection: Conn,
        source: Src,
    ) -> None:
        began = time.monotonic()
        while True:
            try:
                self.take(plugin, context, connection, source)
                return
            except RestartRead as restart:
                if time.monotonic() - began >= restart.within:
                    raise restart.cause
                voided = sum(slot.void() for slot in self.slots)
                logger.warning(
                    f"Reading the inputs again in {restart.wait}s, voiding "
                    f"{voided} landed table(s): {restart.cause}"
                )
                time.sleep(restart.wait)

    def take(
        self,
        plugin: SrcPlugin,
        context: SrcPluginCtx,
        connection: Conn,
        source: Src,
    ) -> None:
        produced = plugin.read_in(
            context,
            connection,
            source,
        )
        expected = len(self.slots)
        if len(produced) > expected or (
            not self.variable and len(produced) != expected
        ):
            raise InputCountError(len(produced), expected)
        try:
            for slot, tables in zip(self.slots, produced):
                for table in tables:
                    slot.land(self.unpack(plugin, slot, table))
        except BaseException:
            for tables in produced:
                close(tables)
            raise

    def unpack(
        self,
        plugin: SrcPlugin,
        slot: Slot,
        table: object,
    ) -> LazyFrameInput:
        if self.legacy:
            if isinstance(table, TableFileInput):
                return LazyFrameInput(
                    pl.scan_parquet(local_file(table.file)),
                    metadata=FrameMetadata(columns=table.metadata),
                )
            raise UnexpectedTableError(
                plugin,
                slot.position,
                table,
                "TableFileInputs",
            )
        if isinstance(table, LazyFrameInput):
            return table
        raise UnexpectedTableError(
            plugin,
            slot.position,
            table,
            "LazyFrameInputs",
        )

    def reap(self) -> list[list[LandedFile]]:
        return [slot.landed for slot in self.slots]


@dataclass
class DestinationSlot(Slot):
    table: pl.LazyFrame | Path


@dataclass(kw_only=True)
class DestinationHandoff(Handoff):
    base_folder: Path
    legacy: bool = False
    source: ColumnPolicy = ColumnPolicy(Zone.LAND)
    target: ColumnPolicy = ColumnPolicy(Zone.EGRESS)
    tables_folder: Path | None = field(default=None, init=False)
    slots: list[DestinationSlot] = field(default_factory=list, init=False)

    def seed(
        self,
        info: Info,
        manifest: OutputManifest,
    ) -> list[DestinationSlot]:
        frames: list[TableFrame] = []
        for slot, entry in enumerate(manifest.outputs):
            if entry.uri is None:
                raise MissingOutputError(slot, entry.name)
            source = TableFile.stored(info.landing_file(slot))
            try:
                frames.append(self.source.scan(source))
            except Exception as error:
                raise OutputReadError(
                    slot,
                    source.uri,
                    str(error),
                ) from error
        names = [entry.name for entry in manifest.outputs]
        if self.legacy:
            self.slots = self.copy(
                info,
                names,
                frames,
            )
        else:
            self.slots = [
                DestinationSlot(
                    name,
                    position,
                    self.target.extract(frame),
                )
                for position, (name, frame) in enumerate(zip(names, frames))
            ]
        return self.slots

    def copy(
        self,
        info: Info,
        names: list[str],
        frames: list[TableFrame],
    ) -> list[DestinationSlot]:
        self.base_folder.mkdir(parents=True, exist_ok=True)
        tables_folder = Path(tempfile.mkdtemp(dir=self.base_folder))
        self.tables_folder = tables_folder
        slots: list[DestinationSlot] = []
        plans: list[pl.LazyFrame] = []
        for position, (name, frame) in enumerate(zip(names, frames)):
            table = tables_folder / f"{position}.parquet"
            try:
                plans.append(self.target.sink(frame, TableFile.local(table)))
            except Exception as error:
                raise OutputCopyError(
                    str(position),
                    info.landing_file(position).uri,
                    str(error),
                ) from error
            slots.append(
                DestinationSlot(
                    name,
                    position,
                    table,
                ),
            )
        try:
            pl.collect_all(plans, engine="streaming")
        except Exception as error:
            raise OutputCopyError(
                ", ".join(str(slot.position) for slot in slots),
                info.function_landing.uri,
                str(error),
            ) from error
        return slots

    def grow(
        self,
        plugin: DestPlugin[Any, Any, Any],
        context: DestContext,
        connection: Conn,
        destination: Dest,
    ) -> None:
        plugin.write_out(
            context,
            connection,
            destination,
            [slot.table for slot in self.slots],
        )

    def reap(self) -> None:
        if self.tables_folder is not None:
            shutil.rmtree(self.tables_folder, ignore_errors=True)
            self.tables_folder = None


def close(tables: Iterable[object]) -> None:
    closing = getattr(tables, "close", None)
    if callable(closing):
        closing()


def local_file(file: Path) -> Path:
    text = str(file)
    if urlparse(text).scheme.lower() == "file":
        return Path(url2pathname(urlparse(text).path))
    return Path(text)
