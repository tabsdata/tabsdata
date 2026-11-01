#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import polars as pl

from tabsdatak.common._configuration.document.loader import DocumentModel
from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.spi import LazyFrameInput, MetadataSpec

LANDING_SUFFIX = ".t"
SCHEMA_SUFFIX = ".s"
TABSDATA_LANDING_FACTS_KEY = "tabsdata.landing.facts"


class SchemaOrigin(Enum):
    DECLARED = "declared"
    INFERRED = "inferred"


class LandingFacts(DocumentModel):
    rows: int
    origin: SchemaOrigin
    metadata: MetadataSpec | None = None


@dataclass(frozen=True)
class LandedPair:
    data: str
    schema: str | None

    @classmethod
    def of(cls, name: str) -> LandedPair:
        if name.endswith(LANDING_SUFFIX):
            return cls(name, f"{name.removesuffix(LANDING_SUFFIX)}{SCHEMA_SUFFIX}")
        return cls(name, None)

    @property
    def names(self) -> list[str]:
        if self.schema is None:
            return [self.data]
        return [self.data, self.schema]


@dataclass(frozen=True)
class LandedTable:
    data: FrameLocator
    schema: FrameLocator

    def facts(self) -> LandingFacts:
        metadata = pl.read_parquet_metadata(
            self.schema.uri,
            storage_options=self.schema.storage_options,
        )
        return LandingFacts.model_validate_json(metadata[TABSDATA_LANDING_FACTS_KEY])

    def inferred(self) -> pl.Schema:
        return self.schema.scan().collect_schema()

    def scan(self) -> tuple[pl.LazyFrame, LandingFacts]:
        facts = self.facts()
        frame = self.data.scan()
        if facts.origin is SchemaOrigin.INFERRED:
            target = self.inferred()
            frame = frame.select(parse(name, dtype) for name, dtype in target.items())
        return frame, facts

    def input(self) -> LazyFrameInput:
        facts = self.facts()
        if facts.origin is SchemaOrigin.INFERRED:
            schema = self.inferred()
        else:
            schema = None
        return LazyFrameInput(self.data.scan(), schema=schema, metadata=facts.metadata)


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
