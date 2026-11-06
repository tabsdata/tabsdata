#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.tableframe._engine.column.generator import (
    GeneratorContext,
    as_expression,
    seed_default,
    temp_column,
)
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_VERSION_SYSTEM_COLUMN_PREFIX,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.column import (
        ColumnDefault as ColumnDefault_,
    )
    from tabsdatak.tableframe.datatypes import ColumnDataType as ColumnDataType_


class IdentifierGenerator:
    def __init__(
        self,
        dtype: ColumnDataType_,
        default: ColumnDefault_,
    ) -> None:
        self.dtype = dtype
        self.default = default

    def apply(
        self,
        lf: pl_.LazyFrame,
        column: str,
        context: GeneratorContext,
    ) -> pl_.LazyFrame:
        import polars as pl
        from polars.plugins import register_plugin_function

        from tabsdata.expansions.tableframe.expressions import PLUGIN_PATH

        if context.is_void:
            return seed_default(lf, column, self.dtype, self.default, context)
        # The plugin makes one id per value it is handed. A literal reaches it as
        # a single value broadcast afterwards, so every row of a batch would share
        # one id; a row index is a real value per row.
        rows = temp_column()
        expression = register_plugin_function(
            plugin_path=PLUGIN_PATH,
            function_name="_identifier_generator",
            args=pl.col(rows),
            kwargs={
                "temp_column": temp_column(),
                "index": context.index,
            },
            is_elementwise=True,
        )
        return (
            lf
            .with_row_index(rows)
            .with_columns_seq(expression.alias(column))
            .drop(rows)
        )


class ConstantVersionGenerator:
    def __init__(
        self,
        dtype: ColumnDataType_,
        default: ColumnDefault_,
    ) -> None:
        self.dtype = dtype
        self.default = default

    def apply(
        self,
        lf: pl_.LazyFrame,
        column: str,
        context: GeneratorContext,
    ) -> pl_.LazyFrame:
        value = self.default
        if context.version is not None:
            version_field = column.removeprefix(TD_VERSION_SYSTEM_COLUMN_PREFIX)
            from_version = getattr(context.version, version_field, None)
            if from_version is not None:
                value = from_version
        return lf.with_columns(as_expression(value, self.dtype).alias(column))
