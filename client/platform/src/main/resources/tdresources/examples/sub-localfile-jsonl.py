#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Write a table to a local JSON Lines file
kind: subscriber
connectors: [local-file-out]
tags: [file, json, jsonl, path-tokens]
summary: >-
  Writes the latest customers table as newline-delimited JSON, one file per
  run: ${EXEC_PLAN_ID} in the path is substituted at write time.
"""

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import DestFileFormat, LocalFileDest


@subscriber(
    destination=LocalFileDest(
        paths=["exports/customers_${EXEC_PLAN_ID}.jsonl"],
        format=DestFileFormat.JSON,
    ),
    input_tables=["crm/customers"],
)
def sub_localfile_jsonl(customers: TableFrameSpec) -> TableFrameSpec:
    return customers
