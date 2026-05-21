"""Core CDC value-types: offset, change records, table identity.

The offset abstract is intentionally narrow -- per-database
ingesters subclass it (GTID, LSN, log+pos) and own the wire
representation."""

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
    """Per-ingester position in the source's change stream.

    Concrete subclasses carry whatever the source needs (a GTID set,
    an LSN, a log+position). The runner persists offsets via
    ``StreamSrcPluginCtx.write_state`` using ``write()`` for the
    on-wire dict shape; ``read()`` is the inverse loader."""

    @abstractmethod
    def update(self, other: CdcOffset) -> None: ...

    @abstractmethod
    def write(self) -> dict: ...

    @classmethod
    @abstractmethod
    def read(cls, data: dict) -> CdcOffset: ...


@dataclass
class CdcDataChange:
    """One CDC row -- an insert, update, or delete -- plus its
    schema-version pointer and an optional offset to commit alongside
    it on flush."""

    operation: str
    transaction: str
    sequence: int
    timestamp: datetime
    table: str
    schema: SchemaVersion
    new_values: dict
    old_values: dict | None
    offset: CdcOffset | None = None

    def __sizeof__(self) -> int:
        # Best-effort byte accounting; the buffer sentinel uses this
        # to decide when to flush by ``buffer_max_bytes``.
        size = object.__sizeof__(self)
        size += sys.getsizeof(self.operation)
        size += sys.getsizeof(self.transaction)
        size += sys.getsizeof(self.sequence)
        size += sys.getsizeof(self.timestamp)
        size += sys.getsizeof(self.table)
        size += sys.getsizeof(self.schema)
        size += sys.getsizeof(self.new_values)
        size += sum(sys.getsizeof(value) for value in self.new_values.values())
        if self.old_values:
            size += sys.getsizeof(self.old_values)
            size += sum(
                sys.getsizeof(value) for value in self.old_values.values()
            )
        if self.offset:
            size += sys.getsizeof(self.offset)
        return size


@dataclass
class CdcSchemaChange:
    """DDL signal -- the source has emitted a schema change for
    ``table`` (or ``None`` for a global event the dialect couldn't
    associate with a specific table). The ingester re-introspects
    and registers a new ``SchemaVersion`` on the next call."""

    table: str | None


CdcChange = Union[CdcDataChange, CdcSchemaChange]


@dataclass(frozen=True)
class TableObject:
    """A (schema, table) identity within the source database."""

    schema: str
    table: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}{DOT}{self.table}"


@dataclass
class TrackedTable:
    """A tracked source table plus the location of its CDC capture
    (where the source emits change rows for it). For databases that
    capture in-place, ``data_table`` and ``cdc_table`` are equal."""

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
    """Marker enum for dialect-specific CDC architectures (binlog,
    WAL, log capture, ...). Concrete subclasses live in per-database
    connectors and feed into ``Dialect.cdc_to_sa``."""

    pass
