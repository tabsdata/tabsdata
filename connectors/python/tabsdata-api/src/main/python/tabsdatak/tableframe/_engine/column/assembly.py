#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.tableframe._engine.column.generator import GeneratorContext
from tabsdatak.tableframe._engine.taxonomy.column import Inception
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.column.registry import Registry as Registry_
    from tabsdatak.tableframe._engine.model.frame import (
        DataVersion as DataVersion_,
    )


def drop_columns(lf: pl_.LazyFrame, columns: list[str]) -> pl_.LazyFrame:
    present = set(lf.collect_schema().names())
    for column in columns:
        if column in present:
            lf = lf.drop(column)
    return lf


def columns_to_drop(registry: Registry_, mode: CreationMode) -> list[str]:
    match mode:
        case CreationMode.RAW:
            optional = set(registry.optional())
            return [name for name in registry.names() if name not in optional]
        case CreationMode.SYS:
            return registry.by_inception(Inception.REGENERATE)
        case CreationMode.MUT:
            return registry.pristine()
        case CreationMode.TAB:
            return []


def add_system_columns(
    lf: pl_.LazyFrame,
    registry: Registry_,
    mode: CreationMode,
    index: int | None = None,
    version: DataVersion_ | None = None,
) -> pl_.LazyFrame:
    lf = drop_columns(lf, columns_to_drop(registry, mode))

    current_columns = set(lf.collect_schema().names())
    is_void = len(current_columns) == 0 and lf.limit(1).collect().height == 0

    optional = set(registry.optional())
    context = GeneratorContext(
        index=index,
        version=version,
        row_index_column=registry.row_index_column(),
        is_void=is_void,
    )

    for name, system_column in registry.metadata().items():
        if name in current_columns or name in optional:
            continue
        if system_column.generator is None:
            continue
        lf = system_column.generator.apply(lf, name, context)
        context = GeneratorContext(
            index=context.index,
            version=context.version,
            row_index_column=context.row_index_column,
            is_void=False,
        )
    return lf
