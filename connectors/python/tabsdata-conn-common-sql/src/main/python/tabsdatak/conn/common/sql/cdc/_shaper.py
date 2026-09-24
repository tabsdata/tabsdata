#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime
from typing import Any

import orjson
import pyarrow as pa

from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._json import orjson_default
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_ORD,
    CDC_META_SCH,
    CDC_META_SEG,
    CDC_META_SQ,
    CDC_META_TS,
    CDC_META_TX,
    DOT,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


class CdcRowShaper:
    def __init__(self, schema: SchemaVersion, cdc_format: CdcFormat):
        self._schema = schema
        self._cdc_format = cdc_format

    @property
    def pa_schema(self) -> pa.Schema:
        return self._schema.pa_schema

    def table(
        self,
        *,
        operation: str,
        transaction: str,
        sequences: list[int],
        timestamp: datetime,
        segment: int,
        ordinals: list[int],
        columns: dict[str, pa.Array],
    ) -> pa.Table:
        rows = len(ordinals)
        fields: dict[str, pa.Array] = {
            CDC_META_OP: self._repeat(operation, pa.utf8(), rows),
            CDC_META_TX: self._repeat(transaction, pa.utf8(), rows),
            CDC_META_SQ: pa.array(sequences, type=pa.int64()),
            CDC_META_TS: self._repeat(timestamp, pa.timestamp("us", tz="UTC"), rows),
            CDC_META_SEG: self._repeat(segment, pa.decimal128(38, 0), rows),
            CDC_META_ORD: pa.array(ordinals, type=pa.decimal128(38, 0)),
            CDC_META_FMT: self._repeat(
                self._cdc_format.columns_format, pa.utf8(), rows
            ),
            CDC_META_FLAT: self._repeat(
                self._cdc_format.flatten_columns, pa.bool_(), rows
            ),
            CDC_META_SCH: self._repeat(self._schema.hash, pa.utf8(), rows),
        }
        fields.update(self._new_image(columns, rows))
        fields.update(self._old_image(rows))
        return pa.table(fields, schema=self.pa_schema)

    def _new_image(
        self, columns: dict[str, pa.Array], rows: int
    ) -> dict[str, pa.Array]:
        values = {
            column.name: columns.get(column.name) or pa.nulls(rows, column.patype)
            for column in self._schema.columns
        }
        if self._cdc_format.flatten_columns:
            return values
        match self._cdc_format.columns_format:
            case "columns":
                return {
                    f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{name}": value
                    for name, value in values.items()
                }
            case "struct":
                return {CDC_DATA_ROW_NEW: self._struct(values)}
            case "map":
                return {CDC_DATA_MAP_NEW: self._map(values, rows)}
            case _:
                raise ValueError(
                    f"Unsupported values format: {self._cdc_format.columns_format!r}"
                )

    def _old_image(self, rows: int) -> dict[str, pa.Array]:
        nulls = {
            column.name: pa.nulls(rows, column.patype)
            for column in self._schema.columns
        }
        match self._cdc_format.columns_format:
            case "columns":
                return {
                    f"{CDC_DATA_COL_OLD_PREFIX}{DOT}{name}": value
                    for name, value in nulls.items()
                }
            case "struct":
                return {CDC_DATA_ROW_OLD: self._struct(nulls)}
            case "map":
                return {CDC_DATA_MAP_OLD: self._map(nulls, rows)}
            case _:
                raise ValueError(
                    f"Unsupported columns_format: {self._cdc_format.columns_format!r}"
                )

    @staticmethod
    def _struct(values: dict[str, pa.Array]) -> pa.Array:
        return pa.StructArray.from_arrays(
            list(values.values()), names=list(values.keys())
        )

    @staticmethod
    def _map(values: dict[str, pa.Array], rows: int) -> pa.Array:
        as_lists = {name: value.to_pylist() for name, value in values.items()}
        return pa.array(
            [
                orjson.dumps(
                    {name: as_lists[name][row] for name in as_lists},
                    default=orjson_default,
                ).decode()
                for row in range(rows)
            ],
            type=pa.utf8(),
        )

    @staticmethod
    def _repeat(value: Any, patype: pa.DataType, rows: int) -> pa.Array:
        return pa.repeat(pa.scalar(value, type=patype), rows)
