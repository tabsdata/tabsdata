#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum

import polars as pl

from tabsdatak.server._tdrunner.function._commons.document.request import (
    LANDING_SUFFIX,
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

SCHEMA_SUFFIX = ".s"
TABSDATA_LANDING_FACTS_KEY = "tabsdata.landing.facts"


# Where the schema of a landed file comes from.
class Origin(Enum):
    # The data's own types: typed data lands as it is.
    DECLARED = "declared"
    # Types polars inferred for untyped data, which lands as strings.
    INFERRED = "inferred"


@dataclass(frozen=True)
class LandedFile:
    position: int
    index: int
    data: Location
    schema: Location
    rows: int
    origin: Origin
    metadata: MetadataSpec | None = None


class LandingError(Exception):
    pass


class NotLandedError(LandingError):
    def __init__(self, record: InputRecord) -> None:
        super().__init__(f"Input record {record} has no landed data and schema")
        self.record = record


class SchemaMismatchError(LandingError):
    def __init__(
        self,
        position: int,
        detail: str,
    ) -> None:
        super().__init__(f"Cannot land input {position}: {detail}")
        self.position = position
        self.detail = detail


class Landing:
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
            origin = Origin.DECLARED
        else:
            check_inferred(position, actual, schema)
            target = schema
            origin = Origin.INFERRED
        data = self.location(position, index, LANDING_SUFFIX)
        landed = TableFile.stored(data)
        landed.sink(frame).collect(engine="streaming")
        rows = landed.stats().row_count
        facts = {
            "rows": rows,
            "origin": origin.value,
            "metadata": metadata,
        }
        description = self.location(position, index, SCHEMA_SUFFIX)
        TableFile.stored(description).sink(
            pl.LazyFrame(schema=target),
            metadata={TABSDATA_LANDING_FACTS_KEY: json.dumps(facts)},
        ).collect()
        return LandedFile(
            position=position,
            index=index,
            data=data,
            schema=description,
            rows=rows,
            origin=origin,
            metadata=metadata,
        )


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


def parse(
    name: str,
    dtype: pl.DataType,
) -> pl.Expr:
    text = pl.col(name)
    if dtype == pl.String:
        return text
    if dtype == pl.Boolean:
        parsed = text.str.to_lowercase().replace_strict(
            {"true": True, "false": False},
            return_dtype=pl.Boolean,
        )
    elif dtype == pl.Date:
        parsed = text.str.to_date(strict=True)
    elif isinstance(dtype, pl.Datetime):
        parsed = text.str.to_datetime(
            time_unit=dtype.time_unit,
            time_zone=dtype.time_zone,
            strict=True,
        )
    elif dtype == pl.Time:
        parsed = text.str.to_time(strict=True)
    else:
        parsed = text.cast(dtype, strict=True)
    return parsed.alias(name)


def scan_landed(
    data: Location,
    schema: Location,
) -> tuple[pl.LazyFrame, MetadataSpec | None]:
    description = TableFile.stored(schema)
    locator = description.locator()
    facts = json.loads(
        pl.read_parquet_metadata(
            locator.uri,
            storage_options=locator.storage_options,
        )[TABSDATA_LANDING_FACTS_KEY],
    )
    frame = TableFile.stored(data).scan()
    if Origin(facts["origin"]) is Origin.INFERRED:
        target = description.scan().collect_schema()
        frame = frame.select(parse(name, dtype) for name, dtype in target.items())
    return frame, facts["metadata"]


def raw_version(info: Info) -> DataVersion:
    return (
        DataVersion
        .builder()
        .with_execution(str(info.execution_id))
        .with_transaction(str(info.transaction_id))
        .with_version(EMPTY_VERSION)
        .with_timestamp(info.triggered_on_datetime)
        .build()
    )


def stamp(
    frame: pl.LazyFrame,
    *,
    idx: int,
    version: DataVersion,
    metadata: MetadataSpec | None,
) -> pl.LazyFrame:
    return TableFrame._build(
        add_connector_columns(frame, metadata),
        CreationMode.RAW,
        idx=idx,
        version=version,
    )._to_polars_lf(drop_system_columns=False)


# Serves a run's landed inputs in manifest order, numbering every landed file
# from one sequence: the input index the landing-data route resolves in
# (position, index) order.
class LandedInputs:
    def __init__(self, info: Info) -> None:
        self.version = raw_version(info)
        self.idx = IdxGenerator()

    def scan(self, record: InputRecord) -> pl.LazyFrame:
        if record.location is None or record.schema is None:
            raise NotLandedError(record)
        frame, metadata = scan_landed(record.location, record.schema)
        return stamp(
            frame,
            idx=self.idx(),
            version=self.version,
            metadata=metadata,
        )
