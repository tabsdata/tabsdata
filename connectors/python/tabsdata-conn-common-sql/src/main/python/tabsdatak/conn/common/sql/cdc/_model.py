#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Union

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow
from tabsdatak.conn.common.sql.cdc.constants import DOT


class CdcOffset(ABC):
    @abstractmethod
    def update(self, other: CdcOffset) -> None: ...

    @abstractmethod
    def write(self) -> dict: ...

    @classmethod
    @abstractmethod
    def read(cls, data: dict) -> CdcOffset: ...


@dataclass
class CdcDataChange:
    operation: str
    transaction: str
    sequence: int
    timestamp: datetime
    table: str
    schema: SchemaVersion
    new_values: dict
    old_values: dict | None
    offset: CdcOffset | None = None
    segment: int = 0
    ordinal: int = 0

    def __sizeof__(self) -> int:
        size = object.__sizeof__(self)
        size += sys.getsizeof(self.operation)
        size += sys.getsizeof(self.transaction)
        size += sys.getsizeof(self.sequence)
        size += sys.getsizeof(self.timestamp)
        size += sys.getsizeof(self.segment)
        size += sys.getsizeof(self.ordinal)
        size += sys.getsizeof(self.table)
        size += sys.getsizeof(self.schema)
        size += sys.getsizeof(self.new_values)
        size += sum(sys.getsizeof(value) for value in self.new_values.values())
        if self.old_values:
            size += sys.getsizeof(self.old_values)
            size += sum(sys.getsizeof(value) for value in self.old_values.values())
        if self.offset:
            size += sys.getsizeof(self.offset)
        return size


@dataclass
class CdcSchemaChange:
    table: str | None


CdcChange = Union[CdcDataChange, CdcSchemaChange]


@dataclass(frozen=True)
class TableObject:
    schema: str
    table: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}{DOT}{self.table}"


@dataclass
class TrackedTable:
    data_table: TableObject
    cdc_table: TableObject
    columns: list[TrackedColumn] = field(default_factory=list)
    pk: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.build_pa_dtypes()

    def build_pa_dtypes(self) -> None:
        if self.columns:
            for column in self.columns:
                column.patype = sqlalchemy_to_arrow(column.satype)

    @property
    def qualified(self) -> str:
        return self.data_table.qualified


class CdcArchitecture(StrEnum):
    pass
