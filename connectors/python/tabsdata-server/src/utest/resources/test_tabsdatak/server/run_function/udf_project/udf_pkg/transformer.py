#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pyarrow.compute as pc

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.datatypes import Int64, String
from tabsdatak.tableframe.udf import on_batch


@on_batch([("doubled", Int64), ("label", String)])
def doubled(ids, names):
    return [pc.multiply(ids, 2), pc.utf8_upper(names)]


@on_batch([("doubled", Int64), ("label", String)])
def short_of_a_row(ids, names):
    return [pc.multiply(ids, 2).slice(1), pc.utf8_upper(names)]


@transformer(input_tables=["c1/t_in"], output_tables=["t_out"])
def udf_transformer(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1.udf(["id", "name"], doubled)


@transformer(input_tables=["c1/t_in"], output_tables=["t_out"])
def broken_udf_transformer(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1.udf(["id", "name"], short_of_a_row)
