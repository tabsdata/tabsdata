#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Filter rows and drop duplicates
kind: transformer
connectors: []
tags: [tableframe, filter, unique, dedupe]
summary: >-
  Drops cancelled and null-keyed events, then keeps the first row per event
  id; unique(subset=...) dedupes on the key rather than on whole rows.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "events": {
        "event_id": [1, 1, 2, 3, None],
        "status": ["ok", "ok", "cancelled", "ok", "ok"],
        "payload": ["a", "a", "b", "c", "d"],
    },
}


@transformer(
    input_tables=["events"],
    output_tables=["events_clean"],
)
def filter_dedupe(events: TableFrameSpec) -> TableFrameSpec:
    if events is None:
        return None
    return (
        events
        .filter(col("event_id").is_not_null() & (col("status") != "cancelled"))
        .unique(subset=["event_id"], keep="first", maintain_order=True)
        .sort("event_id")
    )
