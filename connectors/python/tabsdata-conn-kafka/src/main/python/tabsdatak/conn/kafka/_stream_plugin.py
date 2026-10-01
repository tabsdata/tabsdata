#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source plugin glue.

The run loop is the shared
:class:`~tabsdatak.conn.common._stream._plugin.PipelineStreamSrcPlugin`
one, as for the CDC sources; the ingester and the encoder sit in
``_stream_impl.py``, and the wire-format (de)serialisers in
:mod:`tabsdatak.conn.kafka._serdes`.

Each staged batch is a single-table positional manifest (one slot, the
topic) handed to ``ctx.stage`` along with a callback that commits the
Kafka consumer offset on the broker.

* :class:`KafkaStreamSrcPlugin` -- wires :class:`KafkaStreamSrc` to the
  shared pipeline.
* :data:`KAFKA_STREAM_SRC` -- entry-point :class:`StreamSrcDef`.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._config import StreamRunConfig

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import decode_seconds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._plugin import PipelineStreamSrcPlugin, StreamRun

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.kafka import (
    KafkaStreamSrc,
    KafkaStreamSrcConn,
    SrcCfgKey,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._explorer import KafkaConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    DEFAULT_CONNECTION_TIMEOUT_SECS,
    DEFAULT_CONSUME_TIMEOUT_SECS,
    KafkaEncoder,
    KafkaIngester,
    KafkaPosition,
    KafkaRecord,
)
from tabsdatak.spi import (
    StreamSrcDef,
    StreamStageSrc,
)

logger = logging.getLogger(__name__)


# Allowed ``KafkaStreamSrc.src_cfg`` keys -- mirrors the literal
# annotation on :data:`tabsdatak.conn.kafka.SrcCfgKey` so a typo
# in the runtime key spelling is caught by Pyright.
_KEY_CONSUME_TIMEOUT: SrcCfgKey = "kafka.consume_timeout_seconds"
_KEY_CONNECTION_TIMEOUT: SrcCfgKey = "kafka.connection_timeout_seconds"
_KEY_LOGGING_LEVEL: SrcCfgKey = "kafka.logging_level"
_KEY_BUFFER_MAX_ROWS: SrcCfgKey = "tabsdata.kafka.buffer.max_rows"
_KEY_BUFFER_MAX_BYTES: SrcCfgKey = "tabsdata.kafka.buffer.max_bytes"
_KEY_BUFFER_MAX_SECS: SrcCfgKey = "tabsdata.kafka.buffer.max_secs"
_KEY_TRIGGER_MAX_ROWS: SrcCfgKey = "tabsdata.kafka.trigger.max_rows"
_KEY_TRIGGER_MAX_BYTES: SrcCfgKey = "tabsdata.kafka.trigger.max_bytes"
_KEY_TRIGGER_MAX_SECS: SrcCfgKey = "tabsdata.kafka.trigger.max_secs"


@dataclass(frozen=True, kw_only=True)
class KafkaRunConfig(StreamRunConfig):
    consume_timeout_secs: int = DEFAULT_CONSUME_TIMEOUT_SECS
    connection_timeout_secs: int = DEFAULT_CONNECTION_TIMEOUT_SECS


def _build_kafka_run_config(src: KafkaStreamSrc) -> KafkaRunConfig:
    cfg = dict(src.src_cfg or {})
    return KafkaRunConfig(
        buffer_max_rows=cfg.get(_KEY_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS),
        buffer_max_bytes=cfg.get(_KEY_BUFFER_MAX_BYTES, DEFAULT_BUFFER_MAX_BYTES),
        buffer_max_secs=cfg.get(_KEY_BUFFER_MAX_SECS, DEFAULT_BUFFER_MAX_SECS),
        trigger_max_rows=cfg.get(_KEY_TRIGGER_MAX_ROWS, DEFAULT_TRIGGER_MAX_ROWS),
        trigger_max_bytes=cfg.get(_KEY_TRIGGER_MAX_BYTES, DEFAULT_TRIGGER_MAX_BYTES),
        trigger_max_secs=decode_seconds(
            _KEY_TRIGGER_MAX_SECS,
            cfg.get(_KEY_TRIGGER_MAX_SECS, DEFAULT_TRIGGER_MAX_SECS),
        ),
        poll_interval_seconds=0,
        consume_timeout_secs=int(
            cfg.get(_KEY_CONSUME_TIMEOUT, DEFAULT_CONSUME_TIMEOUT_SECS),
        ),
        connection_timeout_secs=int(
            cfg.get(_KEY_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_SECS),
        ),
        logging_level=int(cfg.get(_KEY_LOGGING_LEVEL, logging.ERROR)),
    )


class KafkaStreamSrcPlugin(
    PipelineStreamSrcPlugin[
        KafkaStreamSrcConn, KafkaStreamSrc, KafkaRecord, KafkaPosition
    ]
):
    """Wires a :class:`KafkaStreamSrc` to the shared stream pipeline."""

    logging_namespace = "tabsdatak.conn.kafka"

    def build_run_config(self, stream_src: KafkaStreamSrc) -> KafkaRunConfig:
        return _build_kafka_run_config(stream_src)

    def build_ingester(
        self, conn: KafkaStreamSrcConn, stream_src: KafkaStreamSrc
    ) -> KafkaIngester:
        cfg = _build_kafka_run_config(stream_src)
        return KafkaIngester(
            conn=conn,
            stream_src=stream_src,
            consume_timeout_secs=cfg.consume_timeout_secs,
            connection_timeout_secs=cfg.connection_timeout_secs,
        )

    def slots(self, run: StreamRun[KafkaRecord, KafkaPosition]) -> Sequence[str]:
        return [run.ingester.topic]

    def build_encoder(self, run: StreamRun[KafkaRecord, KafkaPosition]) -> KafkaEncoder:
        return KafkaEncoder(run.ingester.topic, run.ingester.schema_id_compatibles())


KAFKA_STREAM_SRC = StreamSrcDef(
    conn=KafkaStreamSrcConn,
    type_="kafka-topic-in",
    system="Apache Kafka",
    src_version="v1",
    src=KafkaStreamSrc,
    # One Kafka topic -> one output table; cardinality is always 1.
    cardinality=lambda src: 1,
    plugin=KafkaStreamSrcPlugin,
    # Like the CDC sources, the new port ingests its staged batches
    # through the default stream-stage manifest source.
    publisher_src=StreamStageSrc,
    explorer=KafkaConnectionExplorer,
    icon="conn/kafka/resources/kafka.svg",
)
