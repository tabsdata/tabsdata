#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
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

# Built per-row in the CDC hot path; plain tuples avoid dataclass allocation overhead.
# Each entry is (column_name, type_string_for_hash, raw_type_object_for_mapper).
RawSchema = tuple[tuple[str, str, Any], ...]


def raw_schema_hash(raw_schema: RawSchema) -> str:
    canonical = "|".join(f"{name}:{dtype}" for name, dtype, _ in raw_schema)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def build_pa_schema(
    columns: tuple[TrackedColumn, ...] | list[TrackedColumn],
    cdc_format: CdcFormat,
) -> pa.Schema:
    fields: list[pa.Field] = [
        pa.field(CDC_META_OP, pa.utf8()),
        pa.field(CDC_META_TX, pa.utf8()),
        pa.field(CDC_META_SQ, pa.int64()),
        pa.field(CDC_META_TS, pa.timestamp("us", tz="UTC")),
        pa.field(CDC_META_SEG, pa.decimal128(38, 0)),
        pa.field(CDC_META_ORD, pa.decimal128(38, 0)),
        pa.field(CDC_META_FMT, pa.utf8()),
        pa.field(CDC_META_FLAT, pa.bool_()),
        pa.field(CDC_META_SCH, pa.utf8()),
    ]

    if cdc_format.flatten_columns:
        for column in columns:
            fields.append(pa.field(column.name, column.patype))
    else:
        match cdc_format.columns_format:
            case "columns":
                for column in columns:
                    fields.append(
                        pa.field(
                            f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{column.name}",
                            column.patype,
                        )
                    )
            case "struct":
                struct = [pa.field(column.name, column.patype) for column in columns]
                fields.append(pa.field(CDC_DATA_ROW_NEW, pa.struct(struct)))
            case "map":
                fields.append(pa.field(CDC_DATA_MAP_NEW, pa.utf8()))
            case _:
                raise ValueError(
                    f"Unsupported values format: {cdc_format.columns_format!r}"
                )

    match cdc_format.columns_format:
        case "columns":
            for column in columns:
                fields.append(
                    pa.field(
                        f"{CDC_DATA_COL_OLD_PREFIX}{DOT}{column.name}",
                        column.patype,
                        nullable=True,
                    )
                )
        case "struct":
            struct = [pa.field(column.name, column.patype) for column in columns]
            fields.append(pa.field(CDC_DATA_ROW_OLD, pa.struct(struct), nullable=True))
        case "map":
            fields.append(pa.field(CDC_DATA_MAP_OLD, pa.utf8(), nullable=True))
        case _:
            raise ValueError(
                f"Unsupported columns_format: {cdc_format.columns_format!r}"
            )

    return pa.schema(fields)


@dataclass(frozen=True)
class SchemaVersion:
    version: int
    raw: RawSchema
    hash: str
    columns: tuple[TrackedColumn, ...]
    pa_schema: pa.Schema
