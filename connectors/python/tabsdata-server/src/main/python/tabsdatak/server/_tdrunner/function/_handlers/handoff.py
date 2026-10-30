#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import shutil
import tempfile
from abc import ABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import overload
from urllib.parse import urlparse
from urllib.request import url2pathname

import polars as pl
import pyarrow as pa

from tabsdatak._handoff import dest_handoff, lend_bus, src_handoff
from tabsdatak.server._tdrunner.function._commons.document.request import Info
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
    DestBus,
    DestBusOfArrow,
    DestBusOfDataFrame,
    DestBusOfFile,
    DestBusOfLazyFrame,
    DestChannelOfArrow,
    DestChannelOfDataFrame,
    DestChannelOfFile,
    DestChannelOfLazyFrame,
    DestContext,
    DestPlugin,
    MetadataSpec,
    Src,
    SrcBus,
    SrcBusOfArrow,
    SrcBusOfDataFrame,
    SrcBusOfFile,
    SrcBusOfLazyFrame,
    SrcChannelOfArrow,
    SrcChannelOfDataFrame,
    SrcChannelOfFile,
    SrcChannelOfLazyFrame,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
)
from tabsdatak.tableframe import TableFrame


class HandoffError(Exception):
    pass


class MissingOutputError(HandoffError):
    def __init__(
        self,
        slot: int,
        name: str,
    ) -> None:
        super().__init__(f"Output slot {slot} ({name!r}) has no data")
        self.slot = slot
        self.name = name


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
        super().__init__(f"No handoff exists yet for {type(plugin).__name__}")
        self.plugin = plugin


class ForeignBusError(HandoffError):
    def __init__(
        self,
        plugin: SrcPlugin,
        returned: object,
    ) -> None:
        super().__init__(
            f"{type(plugin).__name__}.read_in returned {returned!r}, not the bus "
            f"it was given",
        )
        self.plugin = plugin
        self.returned = returned


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


class Handoff(ABC):
    @overload
    @staticmethod
    def of(plugin: DestPlugin, base_folder: Path) -> DestinationHandoff: ...

    @overload
    @staticmethod
    def of(plugin: SrcPlugin, base_folder: Path) -> SourceHandoff: ...

    @staticmethod
    def of(
        plugin: SrcPlugin | DestPlugin,
        base_folder: Path,
    ) -> DestinationHandoff | SourceHandoff:
        if isinstance(plugin, DestPlugin):
            bus = dest_handoff(type(plugin))
            return DestinationHandoff(
                base_folder=base_folder,
                bus=DestBusOfFile if bus is None else bus,
                legacy=bus is None,
            )
        if isinstance(plugin, SrcPlugin):
            variant = src_handoff(type(plugin))
            return SourceHandoff(
                bus=SrcBusOfFile if variant is None else variant,
                legacy=variant is None,
            )
        raise UnsupportedPluginError(plugin)


class NamedChannel:
    def __init__(
        self,
        name: str,
        position: int,
    ) -> None:
        self._name = name
        self._position = position

    @property
    def name(self) -> str:
        return self._name

    @property
    def position(self) -> int:
        return self._position


class FrameChannel(NamedChannel):
    def __init__(
        self,
        name: str,
        position: int,
        frame: pl.LazyFrame,
    ) -> None:
        super().__init__(name, position)
        self.frame = frame


class LazyFrameChannel(FrameChannel, DestChannelOfLazyFrame):
    def read(self) -> pl.LazyFrame:
        return self.frame


class DataFrameChannel(FrameChannel, DestChannelOfDataFrame):
    def read(self) -> pl.DataFrame:
        return self.frame.collect(engine="streaming")


class ArrowChannel(FrameChannel, DestChannelOfArrow):
    def read(self) -> pa.RecordBatchReader:
        schema = pl.DataFrame(schema=self.frame.collect_schema()).to_arrow().schema
        chunks = self.frame.collect_batches(lazy=True, engine="streaming")
        return pa.RecordBatchReader.from_batches(
            schema,
            (batch for chunk in chunks for batch in chunk.to_arrow().to_batches()),
        )


class FileChannel(NamedChannel, DestChannelOfFile):
    def __init__(
        self,
        name: str,
        position: int,
        file: Path,
    ) -> None:
        super().__init__(name, position)
        self.file = file

    def read(self) -> Path:
        return self.file


FRAME_CHANNELS: dict[type[DestBus], type[FrameChannel]] = {
    DestBusOfLazyFrame: LazyFrameChannel,
    DestBusOfDataFrame: DataFrameChannel,
    DestBusOfArrow: ArrowChannel,
}


@dataclass(kw_only=True)
class DestinationHandoff(Handoff):
    base_folder: Path
    bus: type[DestBus]
    legacy: bool = False
    source: ColumnPolicy = ColumnPolicy(Zone.LAND)
    target: ColumnPolicy = ColumnPolicy(Zone.EGRESS)
    tables_folder: Path | None = field(default=None, init=False)

    def seed(
        self,
        info: Info,
        manifest: OutputManifest,
    ) -> DestBus:
        frames: list[TableFrame] = []
        for slot, entry in enumerate(manifest.outputs):
            if entry.uri is None:
                raise MissingOutputError(slot, entry.name)
            source = TableFile.stored(info.landing_file(slot))
            try:
                frames.append(self.source.scan(source))
            except Exception as error:
                raise OutputCopyError(
                    str(slot),
                    source.uri,
                    str(error),
                ) from error
        names = [entry.name for entry in manifest.outputs]
        if self.bus is DestBusOfFile:
            return self.bus(
                self.copy(
                    info,
                    names,
                    frames,
                ),
            )
        channel = FRAME_CHANNELS[self.bus]
        return self.bus(
            [
                channel(
                    name,
                    position,
                    self.target.extract(frame),
                )
                for position, (name, frame) in enumerate(zip(names, frames))
            ],
        )

    def copy(
        self,
        info: Info,
        names: list[str],
        frames: list[TableFrame],
    ) -> list[FileChannel]:
        self.base_folder.mkdir(parents=True, exist_ok=True)
        self.tables_folder = Path(tempfile.mkdtemp(dir=self.base_folder))
        channels: list[FileChannel] = []
        plans: list[pl.LazyFrame] = []
        for position, (name, frame) in enumerate(zip(names, frames)):
            table = self.tables_folder / f"{position}.parquet"
            try:
                plans.append(self.target.sink(frame, TableFile.local(table)))
            except Exception as error:
                raise OutputCopyError(
                    str(position),
                    info.landing_file(position).uri,
                    str(error),
                ) from error
            channels.append(
                FileChannel(
                    name,
                    position,
                    table,
                ),
            )
        try:
            pl.collect_all(plans, engine="streaming")
        except Exception as error:
            raise OutputCopyError(
                ", ".join(str(channel.position) for channel in channels),
                info.function_landing.uri,
                str(error),
            ) from error
        return channels

    def grow(
        self,
        plugin: DestPlugin,
        context: DestContext,
        connection: Conn,
        destination: Dest,
        bus: DestBus,
    ) -> None:
        tables = [channel.read() for channel in bus] if self.legacy else bus
        plugin.write_out(
            context,
            connection,
            destination,
            tables,
        )

    def reap(self) -> None:
        if self.tables_folder is not None:
            shutil.rmtree(self.tables_folder, ignore_errors=True)
            self.tables_folder = None


class LandingChannel(NamedChannel):
    def __init__(
        self,
        name: str,
        position: int,
        landing: Landing,
    ) -> None:
        super().__init__(name, position)
        self.landing = landing
        self.landed: list[LandedFile] = []

    def land(
        self,
        frame: pl.LazyFrame,
        schema: pl.Schema | None,
        metadata: MetadataSpec | None,
    ) -> None:
        self.landed.append(
            self.landing.land(
                self.position,
                len(self.landed),
                frame,
                schema=schema,
                metadata=metadata,
            ),
        )


class LazyFrameLandingChannel(LandingChannel, SrcChannelOfLazyFrame):
    def write(
        self,
        data: pl.LazyFrame,
        *,
        schema: pl.Schema | None = None,
        metadata: MetadataSpec | None = None,
    ) -> None:
        self.land(data, schema, metadata)


class DataFrameLandingChannel(LandingChannel, SrcChannelOfDataFrame):
    def write(
        self,
        data: pl.DataFrame,
        *,
        schema: pl.Schema | None = None,
        metadata: MetadataSpec | None = None,
    ) -> None:
        self.land(data.lazy(), schema, metadata)


class ArrowLandingChannel(LandingChannel, SrcChannelOfArrow):
    def write(
        self,
        data: pa.RecordBatchReader,
        *,
        schema: pl.Schema | None = None,
        metadata: MetadataSpec | None = None,
    ) -> None:
        self.land(pl.scan_arrow_c_stream(data), schema, metadata)


class FileLandingChannel(LandingChannel, SrcChannelOfFile):
    def write(
        self,
        data: Path,
        *,
        schema: pl.Schema | None = None,
        metadata: MetadataSpec | None = None,
    ) -> None:
        self.land(pl.scan_parquet(data), schema, metadata)


LANDING_CHANNELS: dict[type[SrcBus], type[LandingChannel]] = {
    SrcBusOfLazyFrame: LazyFrameLandingChannel,
    SrcBusOfDataFrame: DataFrameLandingChannel,
    SrcBusOfArrow: ArrowLandingChannel,
    SrcBusOfFile: FileLandingChannel,
}


@dataclass(kw_only=True)
class SourceHandoff(Handoff):
    bus: type[SrcBus]
    legacy: bool = False
    channels: list[LandingChannel] = field(default_factory=list, init=False)
    variable: bool = field(default=False, init=False)

    def seed(
        self,
        info: Info,
        names: list[str],
        *,
        variable: bool = False,
    ) -> SrcBus:
        self.variable = variable
        landing = Landing(info.function_landing)
        channel = LANDING_CHANNELS[self.bus]
        self.channels = [
            channel(
                name,
                position,
                landing,
            )
            for position, name in enumerate(names)
        ]
        return self.bus(self.channels)

    def grow(
        self,
        plugin: SrcPlugin,
        context: SrcPluginCtx,
        connection: Conn,
        source: Src,
        bus: SrcBus,
    ) -> None:
        if self.legacy:
            self.adopt(
                plugin.read_in(
                    context,
                    connection,
                    source,
                ),
            )
            return
        with lend_bus(plugin, bus):
            returned = plugin.read_in(
                context,
                connection,
                source,
            )
        if returned is not bus:
            raise ForeignBusError(plugin, returned)

    def adopt(self, produced: list[list[TableFileInput]]) -> None:
        expected = len(self.channels)
        if len(produced) > expected or (
            not self.variable and len(produced) != expected
        ):
            raise InputCountError(len(produced), expected)
        for channel, tables in zip(self.channels, produced):
            for table in tables:
                channel.land(
                    pl.scan_parquet(local_file(table.file)),
                    None,
                    table.metadata,
                )

    def reap(self) -> list[list[LandedFile]]:
        return [channel.landed for channel in self.channels]


def local_file(file: Path) -> Path:
    text = str(file)
    if urlparse(text).scheme.lower() == "file":
        return Path(url2pathname(urlparse(text).path))
    return Path(text)
