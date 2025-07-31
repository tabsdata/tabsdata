#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging

import polars as pl

from tabsdata._utils.id import encode_id

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class IdGenerator:
    def __init__(self, index: int):
        self._index = index

    def __call__(
        self, _old_value: pl.String | None = None
    ) -> pl.Expr | str:  # pl.String
        return _id(_old_value)


def _id_default() -> pl.Expr:
    return pl.lit("", pl.String)


def _id(_old_value: pl.String | None = None, debug: bool | None = False) -> str:
    return encode_id(debug=debug)[1]


class IdxGenerator:
    def __init__(self):
        self._index = 0

    def __call__(self) -> int:
        idx = self._index
        self._index += 1
        return idx
