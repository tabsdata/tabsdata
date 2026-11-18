#
# Copyright 2026 Tabsdata Inc.
#

# noinspection PyProtectedMember
from tabsdatak._api import unsafe_seconds
from tabsdatak.api import (
    TableFrameSpec,
    TableFramesSpec,
    TransactionCtx,
    stream_publisher,
)
from tabsdatak.conn.mysql import MySQLCdcStreamSrc
from tabsdatak.tableframe.functions import lit

PUBLISHED_TABLE = "mysql_cdc_published"

CTX_PUBLISHED_TABLE = "mysql_cdc_published_ctx"
CTX_BATCH_KEY = "itest.batch"
CTX_BATCH_COLUMN = "ctx_batch"
CTX_TRANSACTION_COLUMN = "ctx_transaction_id"

TRACKED_TABLE = "td_il_instance"

SEEDED_ROWS = [{"id": index, "name": f"seed-{index}"} for index in range(1, 6)]

SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 2,
}


@stream_publisher(
    source=MySQLCdcStreamSrc(
        tables=[TRACKED_TABLE],
        start_from="full",
        src_cfg=SRC_CFG,
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=1,
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_initial_load_from_mysqlcdc_domain(
    inputs: TableFramesSpec,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None


def _with_ctx(frame: TableFrameSpec, ctx: TransactionCtx) -> TableFrameSpec:
    batch = (ctx.get_attr(CTX_BATCH_KEY) or 0) + 1
    ctx.set_attr(CTX_BATCH_KEY, batch)
    if frame is None:
        return None
    return frame.with_columns(
        lit(batch).alias(CTX_BATCH_COLUMN),
        lit(ctx.transaction_id).alias(CTX_TRANSACTION_COLUMN),
    )


@stream_publisher(
    source=MySQLCdcStreamSrc(
        tables=[TRACKED_TABLE],
        start_from="full",
        src_cfg=SRC_CFG,
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=1,
    output_tables=[CTX_PUBLISHED_TABLE],
)
def pub_stream_publisher_receives_ctx_from_mysqlcdc_domain(
    inputs: TableFramesSpec,
    ctx: TransactionCtx,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return _with_ctx(frames[0] if frames else None, ctx)
