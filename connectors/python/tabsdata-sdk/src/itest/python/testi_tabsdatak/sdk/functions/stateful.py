#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl

from tabsdatak.api import (
    TableFrameSpec,
    TransactionCtx,
    publisher,
    subscriber,
    transformer,
)
from tabsdatak.conn.dev import Data, DataGenSrc
from tabsdatak.conn.localfile import LocalFileDest

SOURCE_COLLECTION = "state_source"
FACTORY_COLLECTION = "state_factory"
PUBLISHED_TABLE = "state_published"
TRANSFORMED_TABLE = "state_transformed"
DELIVERED_FILE = "state_delivered.parquet"
RUNS_COLUMN = "runs"
RUNS_KEY = "lifecycle.runs"


def counted(frame: TableFrameSpec, ctx: TransactionCtx) -> TableFrameSpec:
    runs = (ctx.get_attr(RUNS_KEY) or 0) + 1
    ctx.set_attr(RUNS_KEY, runs)
    return frame.with_columns(pl.lit(runs).alias(RUNS_COLUMN))


@publisher(
    source=DataGenSrc(data=[Data(pl.DataFrame({"id": [1, 2]}))]),
    output_tables=[PUBLISHED_TABLE],
)
def publish(frame: TableFrameSpec, ctx: TransactionCtx) -> TableFrameSpec:
    return counted(frame, ctx)


@transformer(
    input_tables=[f"{SOURCE_COLLECTION}/{PUBLISHED_TABLE}"],
    output_tables=[TRANSFORMED_TABLE],
)
def transform(frame: TableFrameSpec, ctx: TransactionCtx) -> TableFrameSpec:
    return counted(frame, ctx)


@subscriber(
    destination=LocalFileDest(paths=[DELIVERED_FILE]),
    input_tables=[f"{FACTORY_COLLECTION}/{TRANSFORMED_TABLE}"],
)
def deliver(frame: TableFrameSpec, ctx: TransactionCtx) -> TableFrameSpec:
    return counted(frame, ctx)
