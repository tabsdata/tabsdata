#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Union

import pyarrow as pa

from tabsdata.connector.cdc.common._dtype import sqlalchemy_to_arrow
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_OP,
    CDC_META_SQ,
    CDC_META_TX,
    DOT,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec


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


CdcChange = Union[CdcDataChange, CdcSchemaChange]


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
    iname: str | None = None


@dataclass
class TrackedTable:
    data_table: TableObject
    cdc_table: TableObject
    columns: list[TrackedColumn]
    pk: list[str]
    cdc_format: CdcFormatSpec
    pa_schema: pa.Schema = field(init=False)

    def __post_init__(self):
        self._build_pa_dtypes()
        self.pa_schema = self._build_pa_schema()

    def _build_pa_dtypes(self) -> None:
        for column in self.columns:
            column.patype = sqlalchemy_to_arrow(column.satype)

    def _build_pa_schema(self) -> pa.Schema:
        fields: list[pa.Field] = [
            pa.field(CDC_META_OP, pa.utf8()),
            pa.field(CDC_META_TX, pa.utf8()),
            pa.field(CDC_META_SQ, pa.int64()),
        ]

        if self.cdc_format.flat_new_values:
            for column in self.columns:
                fields.append(pa.field(column.name, column.patype))
        else:
            match self.cdc_format.values_format:
                case "columns":
                    for column in self.columns:
                        fields.append(
                            pa.field(
                                f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{column.name}",
                                column.patype,
                            )
                        )
                case "struct":
                    struct = [
                        pa.field(column.name, column.patype) for column in self.columns
                    ]
                    fields.append(pa.field(CDC_DATA_ROW_NEW, pa.struct(struct)))
                case "map":
                    fields.append(pa.field(CDC_DATA_MAP_NEW, pa.utf8()))
                case _:
                    raise ValueError(
                        f"Unsupported values format: {self.cdc_format.values_format!r}"
                    )

        match self.cdc_format.values_format:
            case "columns":
                for column in self.columns:
                    fields.append(
                        pa.field(
                            f"{CDC_DATA_COL_OLD_PREFIX}{DOT}{column.name}",
                            column.patype,
                            nullable=True,
                        )
                    )
            case "struct":
                struct = [
                    pa.field(column.name, column.patype) for column in self.columns
                ]
                fields.append(
                    pa.field(
                        CDC_DATA_ROW_OLD,
                        pa.struct(struct),
                        nullable=True,
                    )
                )
            case "map":
                fields.append(pa.field(CDC_DATA_MAP_OLD, pa.utf8(), nullable=True))
            case _:
                raise ValueError(
                    f"Unsupported values_format: {self.cdc_format.values_format!r}"
                )

        return pa.schema(fields)

    @property
    def qualified(self) -> str:
        return self.data_table.qualified


class CdcArchitecture(StrEnum):
    pass
