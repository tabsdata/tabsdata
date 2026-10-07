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
        section = records[start:stop]
        rows = len(section)
        fields: dict[str, pa.Array] = {
            CDC_META_OP: pa.array([r.operation for r in section], pa.utf8()),
            CDC_META_TX: pa.array([r.transaction for r in section], pa.utf8()),
            CDC_META_SQ: pa.array([r.sequence for r in section], pa.int64()),
            CDC_META_TS: pa.array(
                [r.timestamp.astimezone(timezone.utc) for r in section],
                pa.timestamp("us", tz="UTC"),
            ),
            CDC_META_SEG: pa.array([r.segment for r in section], pa.decimal128(38, 0)),
            CDC_META_ORD: pa.array([r.ordinal for r in section], pa.decimal128(38, 0)),
            CDC_META_FMT: repeated(self._cdc_format.columns_format, pa.utf8(), rows),
            CDC_META_FLAT: repeated(self._cdc_format.flatten_columns, pa.bool_(), rows),
            CDC_META_SCH: repeated(schema.hash, pa.utf8(), rows),
        }
        converters = dict(converters_of(schema))
        new_values = [r.new_values for r in section]
        old_values = [r.old_values for r in section]
        fields.update(self._image(schema, new_values, converters, old=False, rows=rows))
        fields.update(self._image(schema, old_values, converters, old=True, rows=rows))
        return pa.table(fields, schema=schema.pa_schema)

    def _image(
        self,
        schema: SchemaVersion,
        images: list[dict],
        converters: dict[str, Callable[[Any], Any]],
        *,
        old: bool,
        rows: int,
    ) -> dict[str, pa.Array]:
        values: dict[str, list] = {}
        for column in schema.columns:
            name = column.name
            column_values = [image.get(name) for image in images]
            if converter := converters.get(name):
                column_values = [
                    None if value is None else converter(value)
                    for value in column_values
                ]
            values[name] = column_values
        if not old and self._cdc_format.flatten_columns:
            return {
                column.name: pa.array(values[column.name], column.patype)
                for column in schema.columns
            }
        match self._cdc_format.columns_format:
            case "columns":
                prefix = CDC_DATA_COL_OLD_PREFIX if old else CDC_DATA_COL_NEW_PREFIX
                return {
                    f"{prefix}{DOT}{column.name}": pa.array(
                        values[column.name], column.patype
                    )
                    for column in schema.columns
                }
            case "struct":
                field = CDC_DATA_ROW_OLD if old else CDC_DATA_ROW_NEW
                return {
                    field: pa.StructArray.from_arrays(
                        [
                            pa.array(values[column.name], column.patype)
                            for column in schema.columns
                        ],
                        names=[column.name for column in schema.columns],
                    )
                }
            case "map":
                field = CDC_DATA_MAP_OLD if old else CDC_DATA_MAP_NEW
                names = [column.name for column in schema.columns]
                return {
                    field: pa.array(
                        [
                            orjson.dumps(
                                {name: values[name][row] for name in names},
                                default=orjson_default,
                            ).decode()
                            for row in range(rows)
                        ],
                        pa.utf8(),
                    )
                }
            case _:
                raise ValueError(
                    f"Unsupported values format: {self._cdc_format.columns_format!r}"
                )


def repeated(value: Any, patype: pa.DataType, rows: int) -> pa.Array:
    return pa.repeat(pa.scalar(value, type=patype), rows)
