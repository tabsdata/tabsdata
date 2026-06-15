#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from tabsdatak.tableframe._engine.constants.columns.prefixes import (
    TD_VERSION_SYSTEM_COLUMN_PREFIX,
)
from tabsdatak.tableframe._engine.generators import GeneratorContext, temp_column

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.columns import (
        SystemColumnDataType as SystemColumnDataType_,
        SystemColumnDefault as SystemColumnDefault_,
    )


class IdentifierGenerator:
    def apply(
        self,
        lf: pl_.LazyFrame,
        column: str,
        context: GeneratorContext,
    ) -> pl_.LazyFrame:
        import polars as pl
        from polars.plugins import register_plugin_function

        from tabsdata.expansions.tableframe.expressions import PLUGIN_PATH

        expression = register_plugin_function(
            plugin_path=PLUGIN_PATH,
            function_name="_identifier_generator",
            args=pl.col(column),
            kwargs={
                "temp_column": temp_column(),
                "index": context.index,
            },
            is_elementwise=True,
        )
        return lf.with_columns_seq(expression.alias(column))


class ConstantVersionGenerator:
    def __init__(
        self,
        dtype: Callable[[], SystemColumnDataType_],
        default: Callable[[], SystemColumnDefault_],
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

        value = self.default()
        if context.properties is not None:
            property_name = column.removeprefix(TD_VERSION_SYSTEM_COLUMN_PREFIX)
            from_properties = getattr(context.properties, property_name, None)
            if from_properties is not None:
                value = from_properties
        return lf.with_columns(pl.lit(value, dtype=self.dtype()).alias(column))
