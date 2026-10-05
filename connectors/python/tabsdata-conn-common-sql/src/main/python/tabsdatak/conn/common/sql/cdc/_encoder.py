#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterator
from datetime import timezone
from typing import Any

import orjson
import pyarrow as pa

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._encoder import StreamEncoder
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._json import orjson_default
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._utils import TRACE
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
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def converters_of(
    schema: SchemaVersion,
) -> tuple[tuple[str, Callable[[Any], Any]], ...]:
    return tuple(
        (column.name, column.converter)
        for column in schema.columns
        if column.converter is not None
    )


def converted(values: dict | None, converters) -> dict | None:
    if not values or not converters:
        return values
    values = dict(values)
    for name, converter in converters:
        value = values.get(name)
        if value is not None:
            values[name] = converter(value)
    return values


def row_to_record(
    change: CdcDataChange,
    cdc_format: CdcFormat,
    converters: tuple[tuple[str, Callable[[Any], Any]], ...] = (),
) -> dict[str, Any]:
    new_values = converted(change.new_values, converters)
    old_values = converted(change.old_values, converters)
    record: dict[str, Any] = {
        CDC_META_OP: change.operation,
        CDC_META_TX: change.transaction,
        CDC_META_SQ: change.sequence,
        CDC_META_TS: change.timestamp.astimezone(timezone.utc),
        CDC_META_SEG: change.segment,
        CDC_META_ORD: change.ordinal,
        CDC_META_FMT: cdc_format.columns_format,
        CDC_META_FLAT: cdc_format.flatten_columns,
        CDC_META_SCH: change.schema.hash,
    }

    columns = change.schema.columns

    if cdc_format.flatten_columns:
        for column in columns:
            record[column.name] = new_values.get(column.name)
    else:
        match cdc_format.columns_format:
            case "columns":
                for column in columns:
                    record[f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{column.name}"] = (
                        new_values.get(column.name)
                    )
            case "struct":
                record[CDC_DATA_ROW_NEW] = {
                    column.name: new_values.get(column.name) for column in columns
                }
            case "map":
                record[CDC_DATA_MAP_NEW] = orjson.dumps(
                    {column.name: new_values.get(column.name) for column in columns},
                    default=orjson_default,
                ).decode()
            case _:
                raise ValueError(
                    f"Unsupported values format: {cdc_format.columns_format!r}"
                )

    match cdc_format.columns_format:
        case "columns":
            prefix = CDC_DATA_COL_OLD_PREFIX
            for column in columns:
                record[f"{prefix}{DOT}{column.name}"] = old_values.get(column.name)
        case "struct":
            record[CDC_DATA_ROW_OLD] = {
                column.name: old_values.get(column.name) for column in columns
            }
        case "map":
            record[CDC_DATA_MAP_OLD] = orjson.dumps(
                {column.name: old_values.get(column.name) for column in columns},
                default=orjson_default,
            ).decode()
        case _:
            raise ValueError(
                f"Unsupported values format: {cdc_format.columns_format!r}"
            )

    return record


class CdcEncoder(StreamEncoder[CdcChange, CdcOffset]):
    def __init__(self, cdc_format: CdcFormat, tables: dict[str, TrackedTable]):
        self._cdc_format = cdc_format
        self._tables = tables

    def slot(self, record: CdcChange) -> str | None:
        match record:
            case CdcDataChange():
                if record.table not in self._tables:
                    logger.log(
                        TRACE,
                        f"Received change for untracked table {record.table!r}; "
                        "skipping it.",
                    )
                    return None
                return record.table
            case _:
                raise NotImplementedError(
                    f"Event type {type(record).__name__} is not supported yet"
                )

    def size(self, record: CdcDataChange) -> int:
        return sys.getsizeof(record)

    def offset(self, record: CdcDataChange) -> CdcOffset:
        if record.offset is None:
            raise SqlCdcErrorCode.CDC_11.exception(table=record.table)
        return record.offset

    def encode(self, slot: str, records: list[CdcDataChange]) -> Iterator[pa.Table]:
        start = 0
        current_hash = records[0].schema.hash
        for i in range(1, len(records)):
            record_hash = records[i].schema.hash
            if record_hash != current_hash:
                yield self.encode_section(records, start, i)
                start = i
                current_hash = record_hash
        yield self.encode_section(records, start, len(records))

    def encode_section(
        self,
        records: list[CdcDataChange],
        start: int,
        stop: int,
    ) -> pa.Table:
        schema = records[start].schema
        converters = converters_of(schema)
        rows = [
            row_to_record(records[i], self._cdc_format, converters)
            for i in range(start, stop)
        ]
        # noinspection PyArgumentList
        return pa.Table.from_pylist(mapping=rows, schema=schema.pa_schema)
