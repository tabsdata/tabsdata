#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import (
    ConfluentSchemaRegistry,
    KafkaStreamSrcConn,
    SchemaRegistrySpec,
)

TOPIC_PREFIX = "td_it"
ADMIN_TIMEOUT_SECONDS = 16


def servers(domain: RedpandaDomain) -> str:
    return f"{domain.host}:{domain.port}"


def credentials(domain: RedpandaDomain) -> UserPassword | None:
    """The tdx redpanda backend is unauthenticated (plaintext, no SASL), so a
    connection against it carries no credentials at all."""
    if not domain.user:
        return None
    return UserPassword(user=Secret(domain.user), password=Secret(domain.password))


def group_id() -> str:
    """A per-test consumer group, so a run never resumes another one's offsets."""
    return f"g_{uuid.uuid4().hex[:8]}"


def src_conn(
    domain: RedpandaDomain,
    *,
    schema_registry: SchemaRegistrySpec | None = None,
) -> KafkaStreamSrcConn:
    return KafkaStreamSrcConn(
        servers=servers(domain),
        credentials=credentials(domain),
        group_id=group_id(),
        schema_registry=schema_registry,
    )


def confluent_sr(domain: RedpandaDomain) -> ConfluentSchemaRegistry:
    """The Confluent-compatible registry the broker always publishes."""
    return ConfluentSchemaRegistry(
        url=domain.schema_registry_url, credentials=credentials(domain)
    )


class Broker:
    """Admin / producer access to the redpanda backend: how a test creates and seeds
    the topic the connector then consumes, and reaches the registry the
    schema-registry serdes talk to."""

    def __init__(self, domain: RedpandaDomain) -> None:
        self._domain = domain

    @property
    def config(self) -> dict:
        conf = {"bootstrap.servers": servers(self._domain)}
        if self._domain.user:
            conf.update({
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": self._domain.user,
                "sasl.password": self._domain.password,
            })
        return conf

    def create_topic(self, topic: str) -> None:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka.cimpl import NewTopic

        admin = AdminClient(self.config)
        for _topic, future in admin.create_topics([
            NewTopic(topic, num_partitions=1, replication_factor=1)
        ]).items():
            try:
                future.result(timeout=ADMIN_TIMEOUT_SECONDS)
            except Exception as exc:  # noqa: BLE001
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    def delete_topic(self, topic: str) -> None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(self.config)
        for _topic, future in admin.delete_topics([topic]).items():
            try:
                future.result(timeout=ADMIN_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001
                pass

    @contextmanager
    def fresh_topic(self, name: str | None = None) -> Iterator[str]:
        """A topic created for the caller and dropped afterwards. A checked-in
        function names its topic at decoration time, so `name` is dropped first --
        a leftover from an aborted run must not be re-consumed."""
        topic = name or f"{TOPIC_PREFIX}_{uuid.uuid4().hex[:12]}"
        if name:
            self.delete_topic(topic)
        self.create_topic(topic)
        try:
            yield topic
        finally:
            self.delete_topic(topic)

    def produce(self, topic: str, messages: list[dict], serde=None) -> None:
        """Produce `messages` to `topic`, serialised by `serde` -- or as plain JSON
        bytes when none is given, which is what a registry-less JSON source reads."""
        from confluent_kafka import Producer
        from confluent_kafka.serialization import MessageField, SerializationContext

        producer = Producer(self.config)
        ctx = SerializationContext(topic, MessageField.VALUE)
        serialize = serde.serializer() if serde is not None else None
        for message in messages:
            value = (
                serialize(message, ctx)
                if serialize is not None
                else json.dumps(message).encode()
            )
            producer.produce(topic, value=value)
            producer.poll(0)
        producer.flush()

    def schema_registry_client(self):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        conf = {"url": self._domain.schema_registry_url}
        if self._domain.user:
            conf["basic.auth.user.info"] = (
                f"{self._domain.user}:{self._domain.password}"
            )
        return SchemaRegistryClient(conf)


def broker(domain: RedpandaDomain) -> Broker:
    return Broker(domain)
