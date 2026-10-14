#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.kafka import KafkaStreamSrc


@stream_publisher(
    source=KafkaStreamSrc(data_format="json", schema="{}", topic="people"),
    batch_max_secs=60,
    output_tables=["people_streamed"],
)
def my_stream_publisher(t: TableFramesSpec) -> TableFrameSpec:
    return None
