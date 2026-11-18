#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Stream a Kafka topic of JSON messages
kind: stream_publisher
connectors: [kafka-topic-in]
tags: [stream, kafka, json-schema, dlq]
summary: >-
  Consumes a Kafka topic, validating every JSON message against a JSON Schema;
  messages that fail validation go to the dead-letter topic instead of failing
  the batch. A batch closes every 500 messages or, failing that, every five
  minutes (batch_max_rows, batch_max_secs). Stream publishers start when
  registered, take no trigger, and receive each slot as a list of frames.
"""

import json

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.kafka import KafkaStreamSrc
from tabsdatak.tableframe.functions import concat

EVENT_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "event_id": {"type": "integer"},
        "user_id": {"type": "integer"},
        "action": {"type": "string"},
        "occurred_at": {"type": "string"},
    },
    "required": ["event_id", "user_id", "action"],
})


@stream_publisher(
    source=KafkaStreamSrc(
        topic="clickstream",
        data_format="json",
        schema=EVENT_SCHEMA,
        dlq_topic="clickstream_dlq",
    ),
    batch_max_rows=500,
    batch_max_secs=300,
    output_tables=["clickstream"],
)
def stream_kafka_topic(events: TableFramesSpec) -> TableFrameSpec:
    return concat(events or [])
