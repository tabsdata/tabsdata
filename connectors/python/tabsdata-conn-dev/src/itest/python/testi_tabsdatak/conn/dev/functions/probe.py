#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher, subscriber
from tabsdatak.conn.dev import Data, DataGenSrc, ProbeDest

SOURCE_COLLECTION = "probe_source"
PUBLISHED_TABLE = "probe_published"

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


@publisher(
    source=DataGenSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def publish(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@subscriber(
    destination=ProbeDest(),
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
)
def probe(frame: TableFrameSpec) -> TableFrameSpec:
    return frame
