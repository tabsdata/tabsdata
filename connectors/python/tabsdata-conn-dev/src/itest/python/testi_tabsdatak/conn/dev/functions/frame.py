#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher, subscriber
from tabsdatak.conn.dev import Data, DevFrameDest, DevFrameSrc

SOURCE_COLLECTION = "frame_source"
PUBLISHED_TABLE = "frame_published"

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


@publisher(
    source=DevFrameSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def publish(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@subscriber(
    destination=DevFrameDest(),
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
)
def report(frame: TableFrameSpec) -> TableFrameSpec:
    return frame
