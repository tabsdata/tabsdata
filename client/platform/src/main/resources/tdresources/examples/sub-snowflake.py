#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Merge CDC changes into Snowflake
kind: subscriber
connectors: [snowflake-bulk-out]
tags: [warehouse, merge, cdc, idempotent]
summary: >-
  Merges the change rows of a CDC stream onto a Snowflake table by key. The
  stream marks each row in @td.cdc.meta.op (i, u, d, and the l and t reset
  rows, which merge passes through); merge columns take no dots, so the
  subscriber renames it to op and drops the other @td.cdc.* columns. It reads
  @NEW so that no version is skipped when several land between two runs.
  A merge evolves the table only with schema_evolution update and the iceberg
  engine written explicitly, and watermark_column makes a rerun of the same
  transaction a no-op.
"""

from tabsdatak.api import TableFrameSpec, TableFramesSpec, subscriber
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDest
from tabsdatak.tableframe.functions import concat


@subscriber(
    destination=SnowflakeDest(
        tables=["analytics.customers"],
        if_table_exists="merge",
        merge_cfg=[MergeCfg(on=["customer_id"], op_column="op")],
        watermark_column="td_trx_id",
        schema_evolution="update",
        dest_cfg={"snowflake.schema_evolution_engine": "iceberg"},
    ),
    input_tables=["customers_cdc/customers_changes@NEW"],
)
def sub_snowflake(changes: TableFramesSpec) -> TableFrameSpec:
    batch = concat(changes)
    if batch is None:
        return None
    batch = batch.rename({"@td.cdc.meta.op": "op"})
    return batch.drop([name for name in batch.columns if name.startswith("@td.cdc.")])
