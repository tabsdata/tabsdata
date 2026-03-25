#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import orjson

from tabsdata.connector.cdc.common._model import CdcDataChange
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_SCH,
    CDC_META_SQ,
    CDC_META_TX,
    DOT,
)
from tabsdata.connector.cdc.common.typing import CdcFormat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _orjson_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type is not JSON serializable: {type(obj)}")


@dataclass
class CdcBuffer:
    rows: list[CdcDataChange] = field(default_factory=list)
    bytes: int = 0


def row_to_record(
    change: CdcDataChange,
    cdc_format: CdcFormat,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        CDC_META_OP: change.operation,
        CDC_META_TX: change.transaction,
        CDC_META_SQ: change.sequence,
        CDC_META_FMT: cdc_format.values_format,
        CDC_META_FLAT: cdc_format.flatten_values,
        CDC_META_SCH: change.schema.hash,
    }

    columns = change.schema.columns

    if cdc_format.flatten_values:
        for column in columns:
            record[column.name] = change.new_values.get(column.name)
    else:
        match cdc_format.values_format:
            case "columns":
                for column in columns:
                    record[f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{column.name}"] = (
                        change.new_values.get(column.name)
                    )
            case "struct":
                record[CDC_DATA_ROW_NEW] = {
                    column.name: change.new_values.get(column.name)
                    for column in columns
                }
            case "map":
                record[CDC_DATA_MAP_NEW] = orjson.dumps(
                    {
                        column.name: change.new_values.get(column.name)
                        for column in columns
                    },
                    default=_orjson_default,
                ).decode()
            case _:
                raise ValueError(
                    f"Unsupported values format: {cdc_format.values_format!r}"
                )

    match cdc_format.values_format:
        case "columns":
            prefix = CDC_DATA_COL_OLD_PREFIX
            for column in columns:
                record[f"{prefix}{DOT}{column.name}"] = change.old_values.get(
                    column.name
                )
        case "struct":
            record[CDC_DATA_ROW_OLD] = {
                column.name: change.old_values.get(column.name) for column in columns
            }
        case "map":
            record[CDC_DATA_MAP_OLD] = orjson.dumps(
                {column.name: change.old_values.get(column.name) for column in columns},
                default=_orjson_default,
            ).decode()
        case _:
            raise ValueError(f"Unsupported values format: {cdc_format.values_format!r}")

    return record
