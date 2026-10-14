#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, TrxCtx, publisher
from tabsdatak.conn.localfile import LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(source=_SRC, output_tables=["my_table"])
def my_publisher(
    in_1: TableFramesSpec, ctx: TrxCtx
) -> TableFrameSpec:
    import os
    # ctx.work_dir == --out-dir for run-function. Must already exist
    # by the time the user function runs, so a body that writes a
    # sidecar / probes the directory doesn't crash.
    assert os.path.isdir(ctx.work_dir), (
        f"ctx.work_dir {ctx.work_dir} does not exist; runtime "
        f"created out_dir too late"
    )
    return None
