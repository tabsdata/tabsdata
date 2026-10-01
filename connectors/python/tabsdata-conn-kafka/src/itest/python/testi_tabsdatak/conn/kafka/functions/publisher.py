#
# Copyright 2026 Tabsdata Inc.
#

"""Stream publishers used by test_tabsdata_src.py"""

import json

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import unsafe_seconds
from tabsdatak.conn.kafka import KafkaStreamSrc

PUBLISHED_TABLE = "kafka_published"
# A checked-in source fixes its topic at decoration time, so the test seeds this
# very topic rather than a generated one.
TOPIC = "td_it_kafka_stream_publisher"
ROWS = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]

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
        src_cfg={
            "tabsdata.kafka.trigger.max_rows": len(ROWS),
            "kafka.consume_timeout_seconds": 2,
            "tabsdata.kafka.trigger.max_secs": unsafe_seconds(1),
        },
    ),
    flush_interval_mins=1,
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_reads_topic_from_redpanda_domain(
    inputs: TableFramesSpec,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None
