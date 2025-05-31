#
#  Copyright 2025 Tabs Data Inc.
#

import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)


from pathlib import Path
from typing import Iterable

import polars as pl
from polars.plugins import register_plugin_function
from polars.type_aliases import IntoExpr

PLUGIN_PATH = Path(__file__).parent


def dummy(expression: IntoExpr | Iterable[IntoExpr]) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="dummy",
        args=expression,
        is_elementwise=True,
    )
