#
# Copyright 2026 Tabsdata Inc.
#

"""Stream publishers used by test_tabsdata_src.py"""

import json

# noinspection PyProtectedMember
from tabsdatak._api import unsafe_seconds
from tabsdatak.api import (
    TableFrameSpec,
    TableFramesSpec,
    TransactionCtx,
    stream_publisher,
)
from tabsdatak.conn.kafka import KafkaStreamSrc
from tabsdatak.tableframe.functions import lit

PUBLISHED_TABLE = "kafka_published"
# A checked-in source fixes its topic at decoration time, so the test seeds this
# very topic rather than a generated one.
TOPIC = "td_it_kafka_stream_publisher"
ROWS = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]

CTX_PUBLISHED_TABLE = "kafka_published_ctx"
CTX_TOPIC = "td_it_kafka_stream_publisher_ctx"
CTX_BATCH_KEY = "itest.batch"
CTX_BATCH_COLUMN = "ctx_batch"
CTX_TRANSACTION_COLUMN = "ctx_transaction_id"

# KafkaStreamSrc requires a schema even for JSON (self-describing on the wire, but
# the connector types the columns from it).
SCHEMA = json.dumps({
    "title": "Row",
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    "required": ["id", "name"],
})


@stream_publisher(
    source=KafkaStreamSrc(
        topic=TOPIC,
        data_format="json",
        schema=SCHEMA,
        src_cfg={"tabsdata.kafka.poll.max_secs": 2},
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=len(ROWS),
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_reads_topic_from_redpanda_domain(
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
    source=KafkaStreamSrc(
        topic=CTX_TOPIC,
        data_format="json",
        schema=SCHEMA,
        src_cfg={"tabsdata.kafka.poll.max_secs": 2},
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=len(ROWS),
    output_tables=[CTX_PUBLISHED_TABLE],
)
def pub_stream_publisher_receives_ctx_from_redpanda_domain(
    inputs: TableFramesSpec,
    ctx: TransactionCtx,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return _with_ctx(frames[0] if frames else None, ctx)
