#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Medallion gold dimensions and fact
kind: flow
connectors: []
tags: [medallion, gold, dimension, fact, scd2]
summary: >-
  Builds the gold layer from the SCD2 silver tables of
  medallion-cdc-bronze-scd2-silver and the event silver of
  medallion-append-only-event. build_type1_dim gives one row per customer
  with a surrogate entity key. build_type2_dim rebuilds the full version
  history every run from the whole closed history and the open versions,
  both read at @HEAD, so no version is added twice. build_fact pins each
  order to the customer version active when it happened and derives a date
  key; an order whose customer version is not in the history yet is dropped,
  since the join is inner and the orders are read once (@NEW).
  build_fact_table adds them to fct_orders@HEAD, so the table holds every
  fact.
"""

from tabsdatak._medallion.gold.dim import build_type1_dim, build_type2_dim
from tabsdatak._medallion.gold.fact import build_fact_table
from tabsdatak.api import TableFrameSpec, TableFramesSpec, TablesTrigger, transformer


@transformer(
    input_tables=["customers_silver/customers_valid_silver"],
    output_tables=["dim_customer"],
)
def tfr_dim_customer(valid: TableFrameSpec) -> TableFrameSpec:
    return build_type1_dim(valid)


@transformer(
    input_tables=[
        "customers_silver/customers_expired_silver",
        "customers_silver/customers_valid_silver",
    ],
    output_tables=["dim_customer_history"],
    trigger_by=TablesTrigger(tables=["customers_silver/customers_valid_silver"]),
)
def tfr_dim_customer_history(
    expired: TableFrameSpec, valid: TableFrameSpec
) -> TableFrameSpec:
    if valid is None:
        return None
    return build_type2_dim(None, [expired], valid)


@transformer(
    input_tables=[
        "orders_silver/orders_event_silver@NEW",
        "dim_customer_history@HEAD",
        "fct_orders@HEAD",
    ],
    output_tables=["fct_orders"],
    trigger_by=TablesTrigger(tables=["orders_silver/orders_event_silver"]),
)
def tfr_fct_orders(
    orders: TableFramesSpec,
    customer_history: TableFrameSpec,
    fct_head: TableFrameSpec,
) -> TableFrameSpec:
    if customer_history is None:
        return None
    return build_fact_table(
        orders,
        dim_versions=[(customer_history, ["customer_id"], "customer_version_sk")],
        fact_head=fct_head,
    )
