#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Union

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
    dbtype: str
    patype: Any = None


@dataclass
class TrackedTable:
    data_table: TableObject
    cdc_table: TableObject
    columns: list[TrackedColumn]
    pk: list[str]

    @property
    def qualified(self) -> str:
        return self.data_table.qualified

    def solve(self, converter: Callable[[str], Any]) -> TrackedTable:
        solved_columns = [
            TrackedColumn(
                name=column.name,
                dbtype=column.dbtype,
                patype=converter(column.dbtype),
            )
            for column in self.columns
        ]
        return TrackedTable(
            data_table=self.data_table,
            cdc_table=self.cdc_table,
            columns=solved_columns,
            pk=self.pk,
        )


class CdcArchitecture(StrEnum):
    pass
