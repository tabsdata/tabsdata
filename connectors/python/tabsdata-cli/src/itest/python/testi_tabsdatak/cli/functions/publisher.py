#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_metrics.py"""

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc

PUBLISHED_TABLE = "metrics_published"

PUBLISHED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


@publisher(
    source=DataGenSrc(data=[Data(PUBLISHED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_metrics_publishes_three_rows(frame: TableFrameSpec) -> TableFrameSpec:
    return frame
