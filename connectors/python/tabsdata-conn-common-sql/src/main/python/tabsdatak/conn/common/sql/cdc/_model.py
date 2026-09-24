#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Union

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow
from tabsdatak.conn.common.sql.cdc.constants import DOT


class CdcOffset(ABC):
    @abstractmethod
    def update(self, other: CdcOffset) -> None: ...

    @abstractmethod
    def write(self) -> str:
        """Serialize this offset to an opaque string. Each impl owns its
        own encoding; the runner persists the string verbatim and hands
        it back to :meth:`read` on resume."""
        ...

    @classmethod
    @abstractmethod
    def read(cls, data: str) -> CdcOffset:
        """Reconstruct an offset from a string produced by :meth:`write`."""
        ...


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


class InitialLoadSession(ABC):
    """A consistent snapshot of the tracked tables plus the position
    change capture resumes from, opened by
    :meth:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester.open_initial_load`.

    :meth:`tables` yields each tracked table together with the SQLAlchemy
    connection to read it through; the caller reads the table fully
    before asking for the next one, so a per-table snapshot can be closed
    when the generator resumes. :attr:`offset` is valid once every table
    has been yielded.
    """

    @property
    @abstractmethod
    def engine(self) -> Any: ...

    @abstractmethod
    def tables(
        self, tracked_tables: dict[str, TrackedTable]
    ) -> Iterator[tuple[TrackedTable, Any]]: ...

    @property
    @abstractmethod
    def offset(self) -> CdcOffset: ...

    @abstractmethod
    def close(self) -> None: ...


class CdcArchitecture(StrEnum):
    pass
