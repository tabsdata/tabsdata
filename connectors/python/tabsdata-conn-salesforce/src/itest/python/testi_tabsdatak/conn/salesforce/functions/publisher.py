#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_tabsdata_src.py"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.salesforce import SalesforceSrc

PUBLISHED_TABLE = "salesforce_published"
ACCOUNT_LIMIT = 3


@publisher(
    source=SalesforceSrc(
        queries=[f"SELECT Id, Name FROM Account ORDER BY Id LIMIT {ACCOUNT_LIMIT}"]
    ),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_object_from_salesforce_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame
