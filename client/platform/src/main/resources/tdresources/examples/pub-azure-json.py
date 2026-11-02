#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish JSON files from Azure Blob Storage
kind: publisher
connectors: [az-file-in]
tags: [file, json, ndjson, multi-path]
summary: >-
  Reads two newline-delimited JSON paths from Azure Blob Storage into two
  tables; each path is its own slot and arrives as a list of frames.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.azureblob import AzureBlobSrc, SrcFileFormat
from tabsdatak.tableframe.functions import concat


@publisher(
    source=AzureBlobSrc(
        paths=["orders/orders.ndjson", "orders/returns.ndjson"],
        format=SrcFileFormat.JSON,
    ),
    output_tables=["orders", "returns"],
)
def pub_azure_json(
    orders: TableFramesSpec, returns: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return concat(orders or []), concat(returns or [])
