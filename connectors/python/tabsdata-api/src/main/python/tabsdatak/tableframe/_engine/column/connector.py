#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.tableframe import datatypes
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.model.column import SystemColumn
from tabsdatak.tableframe._engine.taxonomy.column import (
    Inception,
    Language,
    SystemColumnCategory,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.spi import MetadataSpec as MetadataSpec_

_DTYPES: tuple[tuple[type, object], ...] = (
    (bool, datatypes.Boolean),
    (int, datatypes.Int64),
    (float, datatypes.Float64),
    (str, datatypes.String),
)


def dtype_of(value: object):
    for value_type, dtype in _DTYPES:
        if isinstance(value, value_type):
            return dtype
    return None


def name_of(key: str) -> str:
    if key.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX):
        return key
    return f"{TD_CONNECTOR_SYSTEM_COLUMN_PREFIX}{key}"


def system_column(name: str, dtype) -> SystemColumn:
    return SystemColumn(
        name=name,
        category=SystemColumnCategory.CONNECTOR,
        dtype=dtype,
        default=None,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    )


def describe(metadata: MetadataSpec_ | None) -> dict[str, SystemColumn]:
    if not metadata:
        return {}

    from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

    described: dict[str, SystemColumn] = {}
    for key, value in metadata.items():
        dtype = dtype_of(value)
        if dtype is None:
            raise TableFrameErrorCode.TF9.exception(
                column=key, type=type(value).__name__
            )
        name = name_of(key)
        described[name] = system_column(name, dtype)
    return described


def add_connector_columns(
    lf: pl_.LazyFrame, metadata: MetadataSpec_ | None
) -> pl_.LazyFrame:
    if not metadata:
        return lf

    import polars as pl

    described = describe(metadata)
    present = set(lf.collect_schema().names())
    values = {name_of(key): value for key, value in metadata.items()}
    columns = [
        pl.lit(values[name], dtype=column.dtype).alias(name)
        for name, column in described.items()
        if name not in present
    ]
    if not columns:
        return lf
    return lf.with_columns(columns)
