#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TrxCtx, subscriber
from tabsdatak.conn.localfile import LocalFileDest

_DEST = LocalFileDest(
    paths=["sink.parquet"],
)


@subscriber(destination=_DEST, input_tables=["c1/t_in"])
def my_subscriber(
    in_1: TableFrameSpec, ctx: TrxCtx
) -> TableFrameSpec:
    seen = ctx.get_attr("smoke.last-seen")
    ctx.set_attr("smoke.last-seen", "sub-after-" + str(seen))
    return in_1
