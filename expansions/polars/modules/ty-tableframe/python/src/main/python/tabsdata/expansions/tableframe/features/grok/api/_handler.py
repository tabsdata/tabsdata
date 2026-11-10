#
# Copyright 2025 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

import polars as pl
from polars._typing import IntoExpr
from polars.plugins import register_plugin_function

from tabsdata.expansions.tableframe.expressions import PLUGIN_PATH
from tabsdatak.tableframe.grok import ColumnDefinition

logger = logging.getLogger()


@dataclass(frozen=True, slots=True)
class GrokParser:
    pattern: str
    schema: dict[str, ColumnDefinition]
    temp_column: str = field(
        init=False, default_factory=lambda: f"__grok_tmp_{uuid.uuid4().hex}"
    )
    index: int = field(init=False, default_factory=lambda: -1)

    @property
    def names(self) -> dict[str, str]:
        return {
            capture: column.name or capture for capture, column in self.schema.items()
        }

    def rust(self, expression: IntoExpr) -> pl.Expr:
        return register_plugin_function(
            plugin_path=PLUGIN_PATH,
            function_name="_grok",
            args=expression,
            kwargs={
                "temp_column": self.temp_column,
                "index": self.index,
                "pattern": self.pattern,
                "mapping": self.names,
            },
            is_elementwise=True,
        )

    def apply(self, lf: pl.LazyFrame, expression: IntoExpr) -> pl.LazyFrame:
        names = self.names
        dtypes = {
            names[capture]: column.dtype or pl.String
            for capture, column in self.schema.items()
        }
        return (
            lf
            .with_columns(self.rust(expression).alias(self.temp_column))
            .unnest(self.temp_column)
            .cast(dtypes)
        )
