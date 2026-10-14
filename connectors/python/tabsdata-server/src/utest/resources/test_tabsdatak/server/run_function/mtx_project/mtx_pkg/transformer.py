#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer


@transformer(
    input_tables=["c1/t_a", "c1/t_b", "c1/t_c"],
    output_tables=["merged"],
)
def multi_transformer(
    a: TableFrameSpec, b: TableFramesSpec, c: TableFrameSpec
) -> TableFrameSpec:
    import polars as pl
    from tabsdatak.tableframe import TableFrame

    frames = []
    if a is not None:
        frames.append(a._to_polars_lf())
    if b:
        frames.extend(f._to_polars_lf() for f in b if f is not None)
    if c is not None:
        frames.append(c._to_polars_lf())
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical_relaxed")
    return TableFrame.from_polars(combined)
