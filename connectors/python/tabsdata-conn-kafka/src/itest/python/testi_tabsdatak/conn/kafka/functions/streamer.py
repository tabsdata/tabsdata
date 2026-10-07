#
# Copyright 2026 Tabsdata Inc.
#

import json

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.kafka import KafkaStreamSrc

SCHEMA = json.dumps({
    "title": "Row",
    "type": "object",
    "properties": {"id": {"type": "integer"}},
    "required": ["id"],
})

TOGGLE_TOPIC = "td_it_kafka_streamer_toggle"
UPDATE_TOPIC = "td_it_kafka_streamer_update"
DELETE_TOPIC = "td_it_kafka_streamer_delete"
MIXED_TOPIC = "td_it_kafka_streamer_mixed"


def _source(topic: str) -> KafkaStreamSrc:
    return KafkaStreamSrc(topic=topic, data_format="json", schema=SCHEMA)


def _first(inputs: TableFramesSpec) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None


@stream_publisher(
    source=_source(TOGGLE_TOPIC), batch_max_secs=60, output_tables=["toggled"]
)
def toggled(inputs: TableFramesSpec) -> TableFrameSpec:
    return _first(inputs)


@stream_publisher(
    source=_source(UPDATE_TOPIC), batch_max_secs=60, output_tables=["updated"]
)
def updated(inputs: TableFramesSpec) -> TableFrameSpec:
    return _first(inputs)


@stream_publisher(
    source=_source(DELETE_TOPIC), batch_max_secs=60, output_tables=["deleted"]
)
def deleted(inputs: TableFramesSpec) -> TableFrameSpec:
    return _first(inputs)


@stream_publisher(
    source=_source(MIXED_TOPIC), batch_max_secs=60, output_tables=["mixed"]
)
def mixed(inputs: TableFramesSpec) -> TableFrameSpec:
    return _first(inputs)
