#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc

SOURCE = LocalFileSrc(paths=["orders_*.parquet"])


@publisher(source=SOURCE, output_tables=["orders_a", "orders_b", "untouched"])
def publish_orders(
    files: TableFramesSpec,
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    first, second = files
    return first.with_columns(pl.lit("published").alias("stage")), second, None
