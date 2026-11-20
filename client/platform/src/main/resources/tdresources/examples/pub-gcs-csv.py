#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish CSV files from Google Cloud Storage
kind: publisher
connectors: [gcs-file-in]
tags: [file, csv, format-cfg]
summary: >-
  Reads semicolon-separated CSV files from a GCS bucket, treating the literal
  NULL as a null value and parsing dates, then keeps only the columns needed.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.gcs import GCSSrc, SrcFileFormat
from tabsdatak.tableframe.functions import concat


@publisher(
    source=GCSSrc(
        paths=["exports/products_*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={
            SrcFileFormat.CSV: {
                "separator": ";",
                "has_header": True,
                "null_values": ["NULL"],
                "try_parse_dates": True,
            }
        },
    ),
    output_tables=["products"],
)
def pub_gcs_csv(products: TableFramesSpec) -> TableFrameSpec:
    frame = concat(products)
    if frame is None:
        return None
    return frame.select("product_id", "name", "category", "price", "released")
