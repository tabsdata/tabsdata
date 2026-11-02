#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Check data quality on an output table
kind: transformer
connectors: []
tags: [data-quality, classifier, filter, summary, fail]
summary: >-
  on_tables=[DataQuality(...)] checks an output table after the function
  succeeds. Classifiers label each row (IsNotNull, IsBetween); operators act
  on the labels: Summary writes one row per classifier to
  orders_clean_dq_summary, Fail aborts the transaction when a tenth of the
  rows or more fail, and Filter moves the failing rows to orders_rejected with
  the verdict columns that explain them.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.dataquality import (
    AnyFailed,
    DataQuality,
    Fail,
    Filter,
    IsBetween,
    IsNotNull,
    PercentThreshold,
    Summary,
)

SAMPLE = {
    "orders": {
        "order_id": [1, 2, 3, 4],
        "customer_id": [10, None, 11, 12],
        "amount": [19.9, 5.0, -3.0, 120.5],
    },
}


@transformer(
    input_tables=["orders"],
    output_tables=["orders_clean"],
    on_tables=[
        DataQuality(
            table="orders_clean",
            classifiers=[
                IsNotNull(["order_id", "customer_id"]),
                IsBetween(["amount"], min_val=0, max_val=100000),
            ],
            operators=[
                Summary(),
                Fail(AnyFailed(), PercentThreshold(10)),
                Filter(
                    AnyFailed(),
                    to_table="orders_rejected",
                    include_quality_columns="criteria",
                ),
            ],
        )
    ],
)
def data_quality(orders: TableFrameSpec) -> TableFrameSpec:
    return orders
