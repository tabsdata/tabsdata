#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Medallion CDC bronze and SCD2 silver
kind: flow
connectors: [postgres-cdc]
tags: [medallion, cdc, bronze, silver, scd2]
summary: >-
  One CDC stream publisher with start_from="full" takes the initial load and
  then every insert, update and delete, with no gap between them; resync it
  with 'tdk fn update --reset-stage'. build_cdc_bronze turns the stream into
  the bronze change stream. The stream publisher lives in the customers_cdc
  collection, with its connection, and the three transformers in
  customers_silver. Silver runs in two stages: build_scd2_silver reads its
  own outputs at
  @HEAD and replaces both every run, the open versions and the whole closed
  history; build_scd2_silver_cleanse then drops the bronze bookkeeping
  columns from the latest version of each.
"""

from tabsdatak._medallion.bronze.cdc import build_cdc_bronze
from tabsdatak._medallion.silver.scd import build_scd2_silver, build_scd2_silver_cleanse
from tabsdatak.api import (
    TableFrameSpec,
    TableFramesSpec,
    TablesTrigger,
    stream_publisher,
    transformer,
)
from tabsdatak.conn.postgres import PostgresCdcStreamSrc
from tabsdatak.tableframe import datatypes
from tabsdatak.tableframe.functions import concat


@stream_publisher(
    source=PostgresCdcStreamSrc(
        tables=["public.customers"],
        replication_slot="td_customers_medallion",
        replication_slot_behavior="create",
        start_from="full",
    ),
    batch_max_secs=120,
    output_tables=["customers_changes"],
)
def stream_customers_changes(changes: TableFramesSpec) -> TableFrameSpec:
    return concat(changes)


@transformer(
    input_tables=["customers_cdc/customers_changes@NEW"],
    output_tables=["customers_bronze"],
)
def tfr_customers_bronze(changes: TableFramesSpec) -> TableFrameSpec:
    return build_cdc_bronze(
        full_commits=[],
        cdc_commits=changes,
        pk=[{"customer_id": datatypes.Int64}],
    )


@transformer(
    input_tables=[
        "customers_bronze@NEW",
        "customers_scd2_valid_raw@HEAD",
        "customers_scd2_expired_raw@HEAD",
    ],
    output_tables=["customers_scd2_valid_raw", "customers_scd2_expired_raw"],
)
def tfr_customers_scd2_change_detect(
    bronze: TableFramesSpec,
    valid_head: TableFrameSpec,
    expired_head: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return build_scd2_silver(
        bronze=bronze, valid_head=valid_head, expired_head=expired_head
    )


@transformer(
    input_tables=[
        "customers_scd2_valid_raw@HEAD",
        "customers_scd2_expired_raw@HEAD",
    ],
    output_tables=["customers_valid_silver", "customers_expired_silver"],
    trigger_by=TablesTrigger(tables=["customers_scd2_valid_raw"]),
)
def tfr_customers_scd2_cleanse(
    valid_raw: TableFrameSpec, expired_raw: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    if valid_raw is None or expired_raw is None:
        return None, None
    return build_scd2_silver_cleanse(valid_raw=[valid_raw], expired_raw=[expired_raw])
