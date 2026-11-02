#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish new Parquet files from S3 with a glob
kind: publisher
connectors: [s3-file-in]
tags: [file, parquet, glob, incremental, last-modified]
summary: >-
  Reads every Parquet object matching a glob in the last path segment.
  initial_last_modified makes it incremental: the first run takes objects
  modified since the cutoff and each later run only the newer ones. A run that
  finds no new objects publishes nothing.
"""

from datetime import datetime, timezone

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.s3 import S3Src
from tabsdatak.tableframe.functions import concat


@publisher(
    source=S3Src(
        paths=["events/*.parquet"],
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ),
    output_tables=["events"],
)
def pub_s3_parquet_glob(events: TableFramesSpec) -> TableFrameSpec:
    return concat(events or [])
