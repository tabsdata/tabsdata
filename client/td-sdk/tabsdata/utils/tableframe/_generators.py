#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

# noinspection PyPackageRequirements
import base32hex
import polars as pl
from uuid_v7.base import uuid7


class IdGenerator:
    def __init__(self, index: int):
        self._index = index

    def __call__(self, _old_value: pl.String | None = None) -> pl.String:
        return _id(_old_value)


def _id_default() -> pl.Expr:
    return pl.lit("", pl.String)


def _id(_old_value: pl.String | None = None) -> pl.String:
    return base32hex.b32encode(uuid7().bytes)[:26]


class IdxGenerator:
    def __init__(self):
        self._index = 0

    def __call__(self) -> int:
        idx = self._index
        self._index += 1
        return idx
