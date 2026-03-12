#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from tabsdata.connector.cdc.common._model import (
    CdcDataChange,
    TrackedTable,
)
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
    COLUMN_BYTE_ESTIMATE,
    DOT,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class CdcBuffer:
    rows: list[CdcDataChange] = field(default_factory=list)
    bytes: int = 0


def row_to_record(
    change: CdcDataChange,
    tracked_table: TrackedTable,
    cdc_format: CdcFormatSpec,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        CDC_META_OP: change.operation,
        CDC_META_TX: change.transaction,
        CDC_META_SQ: change.sequence,
    }

    if cdc_format.flat_new_values:
        for column in tracked_table.columns:
            # ToDo...
            # Not still clear if adding a suffix to completely isolate system generated
            # columns is preferred.
            # record[f"{CDC_DATA_COL_PREFIX}{DOT}{column.name}"] = (
            #     change.new_values.get(column.name)
            # )
            record[column.name] = change.new_values.get(column.name)
    else:
        match cdc_format.values_format:
            case "columns":
                for column in tracked_table.columns:
                    record[f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{column.name}"] = (
                        change.new_values.get(column.name)
                    )
            case "struct":
                record[CDC_DATA_ROW_NEW] = {
                    column.name: change.new_values.get(column.name)
                    for column in tracked_table.columns
                }
            case "map":
                record[CDC_DATA_MAP_NEW] = json.dumps({
                    column.name: change.new_values.get(column.name)
                    for column in tracked_table.columns
                })
            case _:
                raise ValueError(
                    f"Unsupported values format: {cdc_format.values_format!r}"
                )

    match cdc_format.values_format:
        case "columns":
            prefix = CDC_DATA_COL_OLD_PREFIX
            for column in tracked_table.columns:
                record[f"{prefix}{DOT}{column.name}"] = change.old_values.get(
                    column.name
                )
        case "struct":
            record[CDC_DATA_ROW_OLD] = {
                column.name: change.old_values.get(column.name)
                for column in tracked_table.columns
            }
        case "map":
            record[CDC_DATA_MAP_OLD] = json.dumps({
                column.name: change.old_values.get(column.name)
                for column in tracked_table.columns
            })
        case _:
            raise ValueError(f"Unsupported values format: {cdc_format.values_format!r}")

    return record


def estimate_column_bytes(value: Any) -> int:
    if isinstance(value, str):
        return len(value)
    elif isinstance(value, bytes):
        return len(value)
    return COLUMN_BYTE_ESTIMATE


def estimate_row_bytes(change: CdcDataChange) -> int:
    size = sum(estimate_column_bytes(v) for v in change.new_values.values())
    if change.old_values:
        size += sum(estimate_column_bytes(v) for v in change.old_values.values())
    return size
