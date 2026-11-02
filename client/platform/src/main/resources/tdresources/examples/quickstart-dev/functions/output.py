#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest


@subscriber(
    destination=LocalFileDest(paths=["people.jsonl"]),
    input_tables=["dataprod/people"],
)
def qs_sub(people: TableFrameSpec) -> TableFrameSpec:
    return people
