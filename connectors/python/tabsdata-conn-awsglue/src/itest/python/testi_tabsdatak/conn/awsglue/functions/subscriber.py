#
# Copyright 2026 Tabsdata Inc.
#

"""Subscribers used by test_tabsdata_dest.py"""

from publisher import PUBLISHED_TABLE, SOURCE_COLLECTION

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.awsglue import AwsGlueDest

SUBSCRIBED_TABLE = "td_subscribed"


@subscriber(
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
    destination=AwsGlueDest(tables=[SUBSCRIBED_TABLE], if_table_exists="replace"),
)
def sub_subscriber_writes_table_to_awsglue_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
