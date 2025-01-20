#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

# noinspection PyPackageRequirements
import base32hex
import polars as pl
from uuid_v7.base import uuid7


def _id_default() -> pl.Expr:
    return pl.lit("", pl.String)


def _id() -> pl.String:
    return base32hex.b32encode(uuid7().bytes)[:26]
