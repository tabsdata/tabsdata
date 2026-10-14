#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pyarrow.compute as pc

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.dataquality import (
    AnyFailed,
    DataQuality,
    Enrich,
    Fail,
    IsTrue,
    RowCountThreshold,
    Summary,
)
from tabsdatak.tableframe.datatypes import Boolean, Int64, String
from tabsdatak.tableframe.udf import on_batch, on_element



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


CALLS: list[int] = []


@on_element([("doubled", Int64), ("positive", Boolean)])
def counted(identifier):
    CALLS.append(identifier)
    return [identifier * 2, identifier > 1]


def quality(max_failed: int) -> DataQuality:
    return DataQuality(
        table="t_out",
        classifiers=[IsTrue(["positive"])],
        operators=[
            Enrich(to_table="t_out_quality"),
            Summary(table="t_out_quality_summary", trend=None),
            Fail(AnyFailed(), RowCountThreshold(max_failed)),
        ],
    )


@transformer(
    input_tables=["c1/t_in"], output_tables=["t_out"], on_tables=[quality(1)]
)
def quality_udf_transformer(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1.udf("id", counted)


@transformer(
    input_tables=["c1/t_in"], output_tables=["t_out"], on_tables=[quality(0)]
)
def failing_quality_udf_transformer(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1.udf("id", counted)
