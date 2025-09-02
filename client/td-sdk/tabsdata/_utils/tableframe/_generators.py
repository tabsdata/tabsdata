#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import uuid

import polars as pl

from tabsdata._utils.id import encode_id

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class IdGenerator:
    def __init__(self, index: int):
        self._temp_column = f"__tmp_{uuid.uuid4().hex}"
        self._index = index

    def __call__(
        self,
        batch: pl.DataFrame | pl.Series,
        *args,
        **kwargs,
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


def _id_default() -> pl.Expr:
    return pl.lit("", pl.String)


def _id() -> str:
    return encode_id(debug=False)[1]


class IdxGenerator:
    def __init__(self):
        self._index = 0

    def __call__(
        self,
        *args,
        **kwargs,
    ) -> int:
        idx = self._index
        self._index += 1
        return idx
