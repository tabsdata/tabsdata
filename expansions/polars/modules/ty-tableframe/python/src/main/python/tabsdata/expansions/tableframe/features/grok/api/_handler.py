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

import tabsdata.tableframe.functions.col as td_col
from tabsdata.expansions.tableframe.expressions import PLUGIN_PATH

logger = logging.getLogger()


@dataclass(frozen=True, slots=True)
class GrokParser:
    pattern: str
    schema: dict[str, td_col.Column]
    temp_column: str = field(
        init=False, default_factory=lambda: f"__grok_tmp_{uuid.uuid4().hex}"
    )
    index: int = field(init=False, default_factory=lambda: -1)

    def rust(self, expression: IntoExpr) -> pl.Expr:
        mapping = {
            capture: column.name or capture for capture, column in self.schema.items()
        }
        return register_plugin_function(
            plugin_path=PLUGIN_PATH,
            function_name="_grok",
            args=expression,
            kwargs={
                "temp_column": self.temp_column,
                "index": self.index,
                "pattern": self.pattern,
                "mapping": mapping,
            },
            is_elementwise=True,
        )
