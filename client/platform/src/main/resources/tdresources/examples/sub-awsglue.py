#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Append a table to AWS Glue as Iceberg
kind: subscriber
connectors: [awsglue-iceberg-out]
tags: [warehouse, iceberg, athena, append, schema-evolution, idempotent]
summary: >-
  Appends each new version of a table to an Apache Iceberg table on S3 that is
  registered in the AWS Glue Data Catalog, where Amazon Athena and any other
  engine reading the catalog can query it. The table is created on the first
  run, under the bucket and base path of the connection, in a database that
  must already exist. It reads @NEW so that no version is skipped when several
  land between two runs. With schema_evolution update the table gains the
  columns a later version adds. A rerun of the same transaction writes nothing,
  and if_table_exists replace overwrites the table instead of appending to it.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, subscriber
from tabsdatak.conn.awsglue import AwsGlueDest
from tabsdatak.tableframe.functions import concat


@subscriber(
    input_tables=["orders_clean/orders@NEW"],
    destination=AwsGlueDest(
        tables=["sales.orders"],
        if_table_exists="append",
        schema_evolution="update",
    ),
)
def sub_awsglue(orders: TableFramesSpec) -> TableFrameSpec:
    return concat(orders)
