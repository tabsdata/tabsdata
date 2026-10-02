#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka connection explorer.

``check_connect`` and ``metadata`` open a consumer and call
``list_topics`` -- exactly how :class:`KafkaStreamSrcPlugin` proves broker
connectivity at run time. ``metadata`` returns one entity per non-internal
topic with its partition count.

Kafka topics are schemaless at the connection level (the record schema, when
any, is carried per-message via the schema registry / serde), so no columnar
schema is reported and row-level ``data`` preview is left unsupported.
"""

from __future__ import annotations

from dataclasses import field
from typing import ClassVar

import tabsdatak._api as _api
from tabsdatak.conn.common._explorer import BaseConnMetaModel
from tabsdatak.conn.kafka import KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    DEFAULT_CONNECT_TIMEOUT_SECS,
    build_kafka_configs,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.spi import Entity, Explorer


@_api.dataclass(KafkaErrorCode.KAFKA_19)
class KafkaTopicEntity(Entity):
    """A Kafka topic and its partition count."""

    topic: str = ""
    partitions: int = 0

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({"topic": self.topic})

    def field_names(self) -> list[str]:
        return []

    def to_dict(self) -> dict:
        return {"topic": self.topic, "partitions": self.partitions, "fqn": self.fqn()}

    @classmethod
    def from_dict(
        cls, data: dict, connection_type: BaseConnMetaModel
    ) -> KafkaTopicEntity:
        return cls(
            connection_type=connection_type,
            topic=data.get("topic", ""),
            partitions=int(data.get("partitions", 0)),
        )


@_api.dataclass(KafkaErrorCode.KAFKA_20)
class KafkaConnMetaModel(BaseConnMetaModel):
    """Message streaming: a flat namespace of topics."""

    type: str = "messaging"
    description: str = "Message-streaming systems with topics and partitions"
    hierarchy: list[str] = field(default_factory=lambda: ["topic"])
    entity_level: str = "topic"
    entity_fully_qualified_name: list[str] = field(default_factory=lambda: ["topic"])

    def entity_fqn(self, entity: dict) -> str:
        return entity.get("topic", "")


class KafkaConnectionExplorer(Explorer):
    """List the topics reachable from a :class:`KafkaStreamSrcConn`."""

    meta_model: ClassVar[BaseConnMetaModel] = KafkaConnMetaModel()
    entity_type: ClassVar[type[Entity]] = KafkaTopicEntity

    def check_connect(self, conn: KafkaStreamSrcConn) -> None:
        self._cluster_metadata(conn)

    def metadata(self, conn: KafkaStreamSrcConn) -> list[KafkaTopicEntity]:
        md = self._cluster_metadata(conn)
        entities: list[KafkaTopicEntity] = []
        for name, topic_md in md.topics.items():
            # Internal bookkeeping topics (``__consumer_offsets`` etc.).
            if name.startswith("__"):
                continue
            entities.append(
                KafkaTopicEntity(
                    connection_type=self.meta_model,
                    topic=name,
                    partitions=len(topic_md.partitions),
                )
            )
        return entities

    def _cluster_metadata(self, conn: KafkaStreamSrcConn):
        # confluent_kafka is imported lazily so merely importing this module
        # (e.g. a registry scan) doesn't pay for the client library.
        from confluent_kafka import Consumer

        cfg = build_kafka_configs(conn=conn)
        consumer = Consumer(cfg)
        try:
            return consumer.list_topics(timeout=DEFAULT_CONNECT_TIMEOUT_SECS)
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_6.exception(
                cause=exc,
                servers=cfg.get("bootstrap.servers"),
            )
        finally:
            consumer.close()
