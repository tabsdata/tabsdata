#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl

from tabsdatak.server._tdrunner.function._commons.document.message.request import Info
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._handlers.landing import LandedInputs
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import TableMode
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

Input = TableFrame | None
InputSlot = Input | list[Input]


class InputError(Exception):
    pass


class InputScanError(InputError):
    path: str
    detail: str

    def __init__(self, path: str, detail: str) -> None:
        super().__init__(f"Cannot scan {path}: {detail}")
        self.path = path
        self.detail = detail


class SingleSlotError(InputError):
    count: int

    def __init__(self, count: int) -> None:
        super().__init__(f"A single-table slot cannot hold {count} files")
        self.count = count


class Inputs:
    landed: LandedInputs
    publisher: bool

    def __init__(self, info: Info, *, publisher: bool) -> None:
        self.landed = LandedInputs(info)
        self.publisher = publisher

    def slot(self, entry: InputEntry, mode: TableMode) -> InputSlot:
        records = entry.tables
        if mode is TableMode.SINGLE:
            if len(records) == 0:
                return None
            if len(records) == 1:
                return self.table(records[0])
            if self.publisher:
                return self.chunked(records)
            raise SingleSlotError(len(records))
        if entry.is_empty:
            return []
        return [self.table(record) for record in records]

    def table(self, record: InputRecord) -> Input:
        frame = self.scan(record)
        if frame is None:
            return None
        if self.publisher:
            return TableFrame._build(frame, CreationMode.TAB)
        return TableFrame._build(
            frame,
            CreationMode.MUT,
            idx=record.input_idx,
            version=record.version(),
        )

    def chunked(self, records: tuple[InputRecord, ...]) -> Input:
        scans = [
            frame
            for frame in (self.scan(record) for record in records)
            if frame is not None
        ]
        if not scans:
            return None
        return TableFrame._build(self.concat(scans), CreationMode.TAB)

    @classmethod
    def concat(cls, scans: list[pl.LazyFrame]) -> pl.LazyFrame:
        schemas = [scan.collect_schema() for scan in scans]
        unmerged = [
            name
            for name in schemas[0]
            if not cls.merges(
                name,
                [schema[name] for schema in schemas if name in schema],
            )
        ]
        if unmerged:
            scans = [
                scan.with_columns(
                    cls.as_string(name, schema[name])
                    for name in unmerged
                    if name in schema
                )
                for scan, schema in zip(scans, schemas)
            ]
        return pl.concat(scans, how="vertical_relaxed")

    @staticmethod
    def merges(name: str, dtypes: list[pl.DataType]) -> bool:
        try:
            pl.concat(
                [pl.LazyFrame(schema={name: dtype}) for dtype in dtypes],
                how="vertical_relaxed",
            ).collect_schema()
        except pl.exceptions.SchemaError:
            return False
        return True

    @staticmethod
    def as_string(name: str, dtype: pl.DataType) -> pl.Expr:
        column = pl.col(name)
        if not dtype.is_nested():
            return column.cast(pl.String)
        # polars JSON-encodes structs only: wrap the value in {"v": ...} and unwrap.
        encoded = (
            pl.struct(column.alias("v")).struct.json_encode().str.slice(5).str.head(-1)
        )
        return pl.when(column.is_null()).then(None).otherwise(encoded).alias(name)

    def scan(self, record: InputRecord) -> pl.LazyFrame | None:
        if record.metadata_location is not None:
            try:
                return self.landed.scan(record)
            except Exception as error:
                if record.location is not None:
                    path = record.location.uri
                else:
                    path = record.metadata_location.uri
                raise InputScanError(path, str(error)) from error
        source = TableFile.of(record)
        if source is None:
            return None
        try:
            return source.scan()
        except Exception as error:
            raise InputScanError(source.uri, str(error)) from error
