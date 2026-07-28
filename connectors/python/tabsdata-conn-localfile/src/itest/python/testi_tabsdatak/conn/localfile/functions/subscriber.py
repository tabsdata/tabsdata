#
# Copyright 2026 Tabsdata Inc.
#

"""Subscribers used by test_tabsdata_dest.py"""

from publisher import PUBLISHED_TABLE, SOURCE_COLLECTION

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest

SUBSCRIBED_STEM = "td_subscribed"

INPUT_TABLES = [f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"]


@subscriber(
    destination=LocalFileDest(paths=[f"{SUBSCRIBED_STEM}.csv"]),
    input_tables=INPUT_TABLES,
)
def sub_subscriber_writes_csv_to_fs_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame


@subscriber(
    destination=LocalFileDest(paths=[f"{SUBSCRIBED_STEM}.json"]),
    input_tables=INPUT_TABLES,
)
def sub_subscriber_writes_json_to_fs_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame


@subscriber(
    destination=LocalFileDest(paths=[f"{SUBSCRIBED_STEM}.parquet"]),
    input_tables=INPUT_TABLES,
)
def sub_subscriber_writes_parquet_to_fs_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
