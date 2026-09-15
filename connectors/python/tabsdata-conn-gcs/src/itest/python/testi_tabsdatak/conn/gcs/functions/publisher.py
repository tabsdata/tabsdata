#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_tabsdata_src.py and test_tabsdata_dest.py"""

from datetime import datetime, timezone

import polars as pl

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc
from tabsdatak.conn.gcs import GCSSrc

PUBLISHED_TABLE = "gcs_published"
SOURCE_COLLECTION = "gcs_source"

# The world entity the source publishers read. tdx stages it as
# `<entity>_<stamp>.<format>` under a per-format prefix, so the glob names it
# without the stamp and the connection is rooted at the format's prefix.
ENTITY = "city"

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


def _first(frames: TableFramesSpec) -> TableFrameSpec:
    # A file source is array-mode (one path may glob to many objects); every glob
    # here matches a single object.
    matched = [frame for frame in (frames or []) if frame is not None]
    return matched[0] if matched else None


@publisher(
    source=GCSSrc(paths=[f"{ENTITY}_*.csv"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_csv_from_gcs_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=GCSSrc(paths=[f"{ENTITY}_*.ndjson"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_ndjson_from_gcs_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=GCSSrc(paths=[f"{ENTITY}_*.parquet"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_parquet_from_gcs_domain(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)


@publisher(
    source=DataGenSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_subscriber_writes_table_to_gcs_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame


# The baseline the purging publisher starts from. Purge deletes what the
# incremental cursor has moved past, so the source must name one -- without a
# baseline no cursor is kept and the descriptor is refused.
PURGE_CUTOFF = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@publisher(
    source=GCSSrc(
        paths=["*.parquet"],
        initial_last_modified=PURGE_CUTOFF,
        purge=True,
    ),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_purges_what_it_has_consumed(
    frames: TableFramesSpec,
) -> TableFrameSpec:
    return _first(frames)
