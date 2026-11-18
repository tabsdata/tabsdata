#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import (
    TableFrameSpec,
    TableFramesSpec,
    TransactionCtx,
    stream_publisher,
)
from tabsdatak.conn.kafka import KafkaStreamSrc


@stream_publisher(
    source=KafkaStreamSrc(data_format="json", schema="{}", topic="people"),
    batch_max_secs=60,
    output_tables=["people_streamed"],
)
def my_stream_publisher(in_1: TableFramesSpec, ctx: TransactionCtx) -> TableFrameSpec:
    seen = ctx.get_attr("smoke.last-seen")
    ctx.set_attr("smoke.last-seen", "sp-after-" + str(seen))
    return None
