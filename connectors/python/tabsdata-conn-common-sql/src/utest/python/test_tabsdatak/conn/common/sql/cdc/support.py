#
# Copyright 2026 Tabsdata Inc.
#

"""The row-at-a-time reference the encoder is checked against: one record
dict per change, in the layout the stream writes, for Arrow to take apart."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timezone
from typing import Any

import orjson

from tabsdatak.conn.common.sql.cdc._json import orjson_default
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange
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
