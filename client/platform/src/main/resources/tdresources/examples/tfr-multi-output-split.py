#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Split one table into several outputs
kind: transformer
connectors: []
tags: [multi-output, split]
summary: >-
  Routes valid payments and rejected payments to separate tables, returning
  one value per output table. Both are recomputed from all payments on every
  run, so with nothing rejected the rejects table is empty.
"""

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col

SAMPLE = {
    "payments": {
        "payment_id": [1, 2, 3],
        "amount": [10.0, -3.0, 25.0],
        "currency": ["EUR", "EUR", None],
    },
}


@transformer(
    input_tables=["payments"],
    output_tables=["payments_valid", "payments_rejected"],
)
def multi_output_split(
    payments: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec]:
    if payments is None:
        return None, None
    is_valid = (col("amount") > 0) & col("currency").is_not_null()
    valid = payments.filter(is_valid)
    rejected = payments.filter(~is_valid)
    return valid, rejected
