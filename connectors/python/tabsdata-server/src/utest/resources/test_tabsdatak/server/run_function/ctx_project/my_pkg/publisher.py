#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, TransactionCtx, publisher
from tabsdatak.conn.localfile import LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(source=_SRC, output_tables=["my_table"])
def my_publisher(in_1: TableFramesSpec, ctx: TransactionCtx) -> TableFrameSpec:
    seen = ctx.get_attr("my-key")
    new = "from-user-fn" if seen is None else "again-after-" + str(seen)
    ctx.set_attr("my-key", new)
    return None
