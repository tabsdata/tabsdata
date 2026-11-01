#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tabsdatak.common._storage.landed import (
    LANDING_SUFFIX,
    SCHEMA_SUFFIX,
    TABSDATA_LANDING_FACTS_KEY,
    LandedTable,
    LandingFacts,
    SchemaOrigin,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import MetadataSpec
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
    schema: Location
    rows: int
    bytes: int
    origin: SchemaOrigin
    metadata: MetadataSpec | None = None

    @property
    def table(self) -> LandedTable:
        return LandedTable(
            TableFile.stored(self.data).locator(),
            TableFile.stored(self.schema).locator(),
        )

    def record(self) -> InputRecord:
        return InputRecord(location=self.data, schema=self.schema)


class LandingError(Exception):
    pass


class NotLandedError(LandingError):
    record: InputRecord

    def __init__(self, record: InputRecord) -> None:
        super().__init__(f"Input record {record} has no landed data and schema")
        self.record = record


class SchemaMismatchError(LandingError):
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
    def check_inferred(
        position: int,
        actual: pl.Schema,
        inferred: pl.Schema,
    ) -> None:
        if list(actual) != list(inferred):
            raise SchemaMismatchError(
                position,
                f"the schema names columns {list(inferred)} but the data has "
                f"{list(actual)}",
            )
        typed = [name for name, dtype in actual.items() if dtype != pl.String]
        if typed:
            raise SchemaMismatchError(
                position,
                f"a schema is only for untyped data, every column a String, but "
                f"{typed} are typed",
            )

    def land(
        self,
        position: int,
        index: int,
        frame: pl.LazyFrame,
        *,
        schema: pl.Schema | None = None,
        metadata: MetadataSpec | None = None,
    ) -> LandedFile:
        actual = frame.collect_schema()
        if schema is None:
            target = actual
            origin = SchemaOrigin.DECLARED
        else:
            self.check_inferred(position, actual, schema)
            target = schema
            origin = SchemaOrigin.INFERRED
        data = self.location(position, index, LANDING_SUFFIX)
        landed = TableFile.stored(data)
        landed.sink(frame).collect(engine="streaming")
        stats = landed.stats()
        rows = stats.row_count
        facts = LandingFacts(rows=rows, origin=origin, metadata=metadata)
        description = self.location(position, index, SCHEMA_SUFFIX)
        TableFile.stored(description).sink(
            pl.LazyFrame(schema=target),
            metadata={TABSDATA_LANDING_FACTS_KEY: facts.model_dump_json()},
        ).collect()
        return LandedFile(
            position=position,
            index=index,
            data=data,
            schema=description,
            rows=rows,
            bytes=stats.data_bytes,
            origin=origin,
            metadata=metadata,
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
        if record.location is None or record.schema is None:
            raise NotLandedError(record)
        frame, facts = LandedTable(
            TableFile.stored(record.location).locator(),
            TableFile.stored(record.schema).locator(),
        ).scan()
        return self.stamp(frame, facts.metadata)

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
