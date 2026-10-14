#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(source=_SRC, output_tables=["my_table", "untouched"])
def my_publisher(in_1: TableFramesSpec) -> tuple[TableFrameSpec, TableFrameSpec]:
    # Append a marker column to the first input frame; second output
    # is None to exercise the "not modified" branch of the outputs
    # manifest.
    import polars as pl
    if in_1 and in_1[0] is not None:
        first = in_1[0].with_columns(pl.lit("from_user_fn").alias("marker"))
    else:
        first = None
    return (first, None)
