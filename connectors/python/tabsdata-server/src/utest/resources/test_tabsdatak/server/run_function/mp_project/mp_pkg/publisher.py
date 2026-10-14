#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc

_SRC = LocalFileSrc(
    paths=["a.parquet", "b.parquet"],
)


@publisher(source=_SRC, output_tables=["concat"])
def multi_publisher(
    in_a: TableFramesSpec, in_b: TableFramesSpec
) -> TableFrameSpec:
    import polars as pl
    from tabsdatak.tableframe import TableFrame

    frames = []
    for slot in (in_a, in_b):
        if slot:
            frames.extend(f._to_polars_lf() for f in slot if f is not None)
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical_relaxed")
    return TableFrame.from_polars(combined)
