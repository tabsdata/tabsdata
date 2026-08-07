#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.constants import TD_CONNECTOR_COLUMN_PREFIX

KAFKA_META_PREFIX: str = TD_CONNECTOR_COLUMN_PREFIX + "kafka."
"""Name-prefix (`"@td.kafka."`) shared by the metadata the Kafka stream
source writes into the batches it stages."""

KAFKA_META_TOPIC: str = KAFKA_META_PREFIX + "topic"
"""Metadata column (`"@td.kafka.topic"`) written into every batch
`KafkaStreamSrc` stages, holding the topic the message was consumed
from. A stream source stages parquet rather than reporting per-file
metadata, so the column is written into the data itself -- the same way
the CDC sources write their `@td.cdc.meta.*` columns."""
