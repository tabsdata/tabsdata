#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_tabsdata_src.py and test_tabsdata_dest.py"""

import polars as pl

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.azureblob import AzureBlobSrc
from tabsdatak.conn.dev import Data, DataGenSrc

PUBLISHED_TABLE = "azureblob_published"
SOURCE_COLLECTION = "azureblob_source"

# The world entity the source publishers read. tdx stages it as
# `<entity>_<stamp>.<format>` under a per-format prefix, so the glob names it
# without the stamp and the connection is rooted at the format's prefix.
ENTITY = "city"

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


def _first(frames: TableFramesSpec) -> TableFrameSpec:
    # A file source is array-mode (one path may glob to many blobs); every glob
    # here matches a single blob.
    matched = [frame for frame in (frames or []) if frame is not None]
    return matched[0] if matched else None


@publisher(
    source=AzureBlobSrc(paths=[f"{ENTITY}_*.csv"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_csv_from_azureblob_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=AzureBlobSrc(paths=[f"{ENTITY}_*.ndjson"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_ndjson_from_azureblob_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=AzureBlobSrc(paths=[f"{ENTITY}_*.parquet"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_parquet_from_azureblob_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=DataGenSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_subscriber_writes_table_to_azureblob_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
