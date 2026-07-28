#
# Copyright 2026 Tabsdata Inc.
#

"""Subscribers used by test_tabsdata_dest.py"""

from publisher import PUBLISHED_TABLE, SOURCE_COLLECTION

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.postgres import PostgresDest

SUBSCRIBED_TABLE = "td_subscribed"


@subscriber(
    destination=PostgresDest(tables=[SUBSCRIBED_TABLE], if_table_exists="replace"),
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
)
def sub_subscriber_writes_table_to_postgres_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
