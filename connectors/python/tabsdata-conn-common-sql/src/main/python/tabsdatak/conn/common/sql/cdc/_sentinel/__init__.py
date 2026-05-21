"""In-memory CDC buffer + per-row record builder.

The runner threads changes from the ingester through a
``BufferSentinel`` (rows-bytes-secs aware) which flushes to a
``DiskSentinel`` (per-table parquet files) which in turn calls
``StreamSrcPluginCtx`` to stage + commit + trigger downstream."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timezone
from decimal import Decimal
from typing import Any

import orjson

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
    CDC_META_SCH,
    CDC_META_SQ,
    CDC_META_TS,
    CDC_META_TX,
    DOT,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)


def _orjson_default(obj: Any) -> Any:
    """``orjson`` does not natively encode Decimal -- float-coerce
    so the JSON-encoded ``map`` payload stays roundtrippable."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type is not JSON serializable: {type(obj)}")


@dataclass
class CdcBuffer:
    """One in-memory accumulator per tracked table -- the buffer
    sentinel holds a ``dict[table, CdcBuffer]`` and drains it on
    flush."""

    rows: list[CdcDataChange] = field(default_factory=list)
    bytes: int = 0


def row_to_record(
    change: CdcDataChange,
    cdc_format: CdcFormat,
) -> dict[str, Any]:
    """Turn one ``CdcDataChange`` into the dict ``pa.Table.from_pylist``
    consumes. Layout follows ``cdc_format`` -- meta columns first,
    then new columns (inline when ``flatten_columns`` is set; otherwise
    in the chosen ``columns_format``), then old columns (always in the
    chosen ``columns_format``)."""
    record: dict[str, Any] = {
        CDC_META_OP: change.operation,
        CDC_META_TX: change.transaction,
        CDC_META_SQ: change.sequence,
        CDC_META_TS: change.timestamp.astimezone(timezone.utc),
        CDC_META_FMT: cdc_format.columns_format,
        CDC_META_FLAT: cdc_format.flatten_columns,
        CDC_META_SCH: change.schema.hash,
    }

    columns = change.schema.columns

    if cdc_format.flatten_columns:
        for column in columns:
            record[column.name] = change.new_values.get(column.name)
    else:
        match cdc_format.columns_format:
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
                    f"Unsupported columns format: {cdc_format.columns_format!r}"
                )

    old = change.old_values or {}
    match cdc_format.columns_format:
        case "columns":
            prefix = CDC_DATA_COL_OLD_PREFIX
            for column in columns:
                record[f"{prefix}{DOT}{column.name}"] = old.get(column.name)
        case "struct":
            record[CDC_DATA_ROW_OLD] = {
                column.name: old.get(column.name) for column in columns
            }
        case "map":
            record[CDC_DATA_MAP_OLD] = orjson.dumps(
                {column.name: old.get(column.name) for column in columns},
                default=_orjson_default,
            ).decode()
        case _:
            raise ValueError(
                f"Unsupported columns format: {cdc_format.columns_format!r}"
            )

    return record
