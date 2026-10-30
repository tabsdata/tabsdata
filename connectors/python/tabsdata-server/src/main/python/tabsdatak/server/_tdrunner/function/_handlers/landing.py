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
    Location,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.spi import MetadataSpec

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
