#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish a local CSV file
kind: publisher
connectors: [local-file-in]
tags: [file, csv]
summary: >-
  Reads a CSV file under the connection's base path into a table. File sources
  hand each path to the function as a list of frames (one per matched file),
  so the function folds it with concat. Each run reads the whole file, so
  incremental=False, and initial_last_modified takes the file only when it
  changed: a run that finds it unchanged leaves the table as it is.
"""

from datetime import datetime, timezone

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc, SrcFileFormat
from tabsdatak.tableframe.functions import concat


@publisher(
    source=LocalFileSrc(
        paths=["customers.csv"],
        format=SrcFileFormat.CSV,
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        incremental=False,
    ),
    output_tables=["customers"],
)
def pub_localfile_csv(customers: TableFramesSpec) -> TableFrameSpec:
    return concat(customers)
