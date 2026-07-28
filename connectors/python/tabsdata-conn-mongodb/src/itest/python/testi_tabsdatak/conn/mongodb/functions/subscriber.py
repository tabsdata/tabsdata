#
# Copyright 2026 Tabsdata Inc.
#

"""Subscribers used by test_tabsdata_dest.py"""

from publisher import PUBLISHED_TABLE, SOURCE_COLLECTION

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.mongodb import MongoDBDest

SUBSCRIBED_COLLECTION = "td_subscribed"


@subscriber(
    destination=MongoDBDest(
        collections_with_ids=[(SUBSCRIBED_COLLECTION, None)],
        if_collection_exists="replace",
    ),
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
)
def sub_subscriber_writes_collection_to_mongodb_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
