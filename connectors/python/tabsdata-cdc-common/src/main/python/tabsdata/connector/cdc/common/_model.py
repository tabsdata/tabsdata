#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Union

from tabsdata.connector.cdc.common._dtype import sqlalchemy_to_arrow
from tabsdata.connector.cdc.common.constants import DOT


@dataclass
class CdcDataChange:
    operation: str
    transaction: str
    sequence: int
    table: str
    new_values: dict
    old_values: dict | None


@dataclass
class CdcSchemaChange:
    table: str | None


CdcEvent = Union[CdcDataChange, CdcSchemaChange]


@dataclass(frozen=True)
class TableObject:
    schema: str
    table: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}{DOT}{self.table}"


@dataclass
class TrackedColumn:
    name: str
    satype: Any
    patype: Any = None


@dataclass
class TrackedTable:
    data_table: TableObject
    cdc_table: TableObject
    columns: list[TrackedColumn]
    pk: list[str]

    def __post_init__(self):
        for column in self.columns:
            column.patype = sqlalchemy_to_arrow(column.satype)

    @property
    def qualified(self) -> str:
        return self.data_table.qualified


class CdcArchitecture(StrEnum):
    pass
