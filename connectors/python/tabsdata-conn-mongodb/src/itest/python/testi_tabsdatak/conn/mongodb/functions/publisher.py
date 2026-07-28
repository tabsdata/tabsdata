#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_tabsdata_dest.py"""

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc

PUBLISHED_TABLE = "mongodb_published"
SOURCE_COLLECTION = "mongodb_source"

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


@publisher(
    source=DataGenSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_subscriber_writes_collection_to_mongodb_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
