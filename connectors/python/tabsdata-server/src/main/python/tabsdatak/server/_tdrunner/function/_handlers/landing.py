#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tabsdatak.common._storage.failures import collected
from tabsdatak.common._storage.landed import (
    LANDING_SUFFIX,
    METADATA_SUFFIX,
    LandedTable,
    StoredMetadata,
    StoredMetadataError,
    mismatch,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import FrameMetadata, LazyFrameInput, MetadataSpec
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.column.connector import add_connector_columns
from tabsdatak.tableframe._engine.column.generator import IdxGenerator
from tabsdatak.tableframe._engine.constant.emptiness import EMPTY_VERSION
from tabsdatak.tableframe._engine.model.frame import DataVersion
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode


@dataclass(frozen=True)
class LandedFile:
    position: int
    index: int
    data: Location
    metadata: Location
    rows: int
    bytes: int

    @property
    def table(self) -> LandedTable:
        return LandedTable(
            TableFile.stored(self.data).locator(),
            TableFile.stored(self.metadata).locator(),
        )

    def record(self) -> InputRecord:
        return InputRecord(location=self.data, metadata_location=self.metadata)


class LandingError(Exception):
    pass


class NotLandedError(LandingError):
    record: InputRecord

    def __init__(self, record: InputRecord) -> None:
        super().__init__(f"Input record {record} has no landed data and metadata")
        self.record = record


class MetadataMismatchError(LandingError):
    position: int
    detail: str

    def __init__(
        self,
        position: int,
        detail: str,
    ) -> None:
        super().__init__(f"Cannot land input {position}: {detail}")
        self.position = position
        self.detail = detail


class Landing:
    base: Location

    def __init__(self, base: Location) -> None:
        self.base = base

    def location(
        self,
        position: int,
        index: int,
        suffix: str,
    ) -> Location:
        root = self.base.uri.rstrip("/")
        return Location(
            uri=f"{root}/{position}.{index}{suffix}",
            env_prefix=self.base.env_prefix,
        )

    @staticmethod
    def check(
        position: int,
        actual: pl.Schema,
        metadata: FrameMetadata,
    ) -> None:
        detail = mismatch(actual, metadata)
        if detail is not None:
            raise MetadataMismatchError(position, detail)

    def land(
        self,
        position: int,
        index: int,
        table: LazyFrameInput,
    ) -> LandedFile:
        actual = table.frame.collect_schema()
        if isinstance(table.metadata, FrameMetadata):
            self.check(position, actual, table.metadata)
        data = self.location(position, index, LANDING_SUFFIX)
        landed = TableFile.stored(data)
        collected(landed.sink(table.frame), engine="streaming")
        if table.metadata is None:
            metadata = FrameMetadata()
        elif isinstance(table.metadata, FrameMetadata):
            metadata = table.metadata
        else:
            metadata = table.metadata()
            self.check(position, actual, metadata)
        try:
            stored = StoredMetadata.of(metadata)
        except StoredMetadataError as error:
            raise MetadataMismatchError(position, str(error)) from error
        description = self.location(position, index, METADATA_SUFFIX)
        TableFile.stored(description).sink(stored.lazy()).collect()
        stats = landed.stats()
        return LandedFile(
            position=position,
            index=index,
            data=data,
            metadata=description,
            rows=stats.row_count,
            bytes=stats.data_bytes,
        )


class LandedInputs:
    version: DataVersion
    idx: IdxGenerator

    def __init__(self, info: Info) -> None:
        self.version = (
            DataVersion
            .builder()
            .with_execution(str(info.execution_id))
            .with_transaction(str(info.transaction_id))
            .with_version(EMPTY_VERSION)
            .with_timestamp(info.triggered_on_datetime)
            .build()
        )
        self.idx = IdxGenerator()

    def scan(self, record: InputRecord) -> pl.LazyFrame:
        if record.location is None or record.metadata_location is None:
            raise NotLandedError(record)
        frame, metadata = LandedTable(
            TableFile.stored(record.location).locator(),
            TableFile.stored(record.metadata_location).locator(),
        ).scan()
        return self.stamp(frame, metadata.columns)

    def stamp(
        self,
        frame: pl.LazyFrame,
        metadata: MetadataSpec | None,
    ) -> pl.LazyFrame:
        return TableFrame._build(
            add_connector_columns(frame, metadata),
            CreationMode.RAW,
            idx=self.idx(),
            version=self.version,
        )._to_polars_lf(drop_system_columns=False)
