#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import uuid
from typing import Iterable, Union

import polars as pl

# noinspection PyProtectedMember
from polars._typing import IntoExpr
from polars.plugins import register_plugin_function

from tabsdata._utils.id import encode_id
from tabsdata.expansions.tableframe.expressions import PLUGIN_PATH

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class IdGenerator:
    def __init__(self, index: int):
        self._temp_column = f"__tmp_{uuid.uuid4().hex}"
        self._index = index

    def python(
        self,
        batch: pl.DataFrame | pl.Series,
    ) -> pl.DataFrame | pl.Series:
        n = batch.len() if isinstance(batch, pl.Series) else batch.height

        if n == 0:
            empty = pl.Series(self._temp_column, [], dtype=pl.String)
            if isinstance(batch, pl.Series):
                return empty
            return batch.with_columns(empty)

        column = [_id() for _ in range(n)]
        output = pl.Series(self._temp_column, column, dtype=pl.String)

        if isinstance(batch, pl.Series):
            return output
        return batch.with_columns(output)

    def rust(self, expression: Union[IntoExpr, Iterable[IntoExpr]]) -> pl.Expr:
        return register_plugin_function(
            plugin_path=PLUGIN_PATH,
            function_name="_identifier_generator",
            args=expression,
            kwargs={
                "temp_column": self._temp_column,
                "index": self._index,
            },
            is_elementwise=True,
        )


def _id_default() -> pl.Expr:
    return pl.lit("", pl.String)


def _id() -> str:
    return encode_id(debug=False)[1]


class IdxGenerator:
    def __init__(self):
        self._index = 0

    def __call__(
        self,
    ) -> int:
        idx = self._index
        self._index += 1
        return idx
