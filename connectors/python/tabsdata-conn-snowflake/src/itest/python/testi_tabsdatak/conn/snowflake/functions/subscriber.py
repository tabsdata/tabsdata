#
# Copyright 2026 Tabsdata Inc.
#

"""Subscribers used by test_tabsdata_dest.py"""

from publisher import PUBLISHED_TABLE, SOURCE_COLLECTION

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.snowflake import SnowflakeDest

# Upper case because the connector quotes the name it is given, and Snowflake
# stores unquoted identifiers upper-folded.
SUBSCRIBED_TABLE = "TD_SUBSCRIBED"


@subscriber(
    destination=SnowflakeDest(tables=[SUBSCRIBED_TABLE], if_table_exists="replace"),
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
)
def sub_subscriber_writes_table_to_snowflake_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
