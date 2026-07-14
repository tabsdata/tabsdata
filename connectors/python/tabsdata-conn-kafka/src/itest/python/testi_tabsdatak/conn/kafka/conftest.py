#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Kafka test fixtures for this package's integration and e2e suites: a
thin client over whichever redpanda backend the test declared (its ``domain``) to
create/seed/drop topics and reach the schema registry, plus a fabricated
``StreamSrcPluginCtx`` for the integration suite.

Kafka is a streaming *source* connector (``KafkaStreamSrc``, no Dest). The tdx
``redpanda`` backend is unauthenticated (plaintext, no SASL) and publishes a
Confluent-compatible schema registry whose URL the domain now exposes as
``schema_registry_url``."""

from __future__ import annotations

import json
import uuid

import pytest


def _producer_config(domain) -> dict:
    # tdx redpanda is unauthenticated -> plaintext, no SASL.
    return {"bootstrap.servers": f"{domain.host}:{domain.port}"}


def _create_topic(domain, topic: str) -> None:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient(_producer_config(domain))
    for _t, fut in admin.create_topics([
        NewTopic(topic, num_partitions=1, replication_factor=1)
    ]).items():
        try:
            fut.result(timeout=16)
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def _delete_topic(domain, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_producer_config(domain))
    for _t, fut in admin.delete_topics([topic]).items():
        try:
            fut.result(timeout=16)
        except Exception:  # noqa: BLE001
            pass


def _produce_json(domain, topic: str, messages: list[dict]) -> None:
    from confluent_kafka import Producer

    producer = Producer(_producer_config(domain))
    for msg in messages:
        producer.produce(topic, value=json.dumps(msg).encode())
        producer.poll(0)
    producer.flush()


class _Kafka:
    """Broker helpers bound to the redpanda backend the test declared."""

    def __init__(self, backend) -> None:
        self._backend = backend

    @property
    def servers(self) -> str:
        return f"{self._backend.host}:{self._backend.port}"

    @property
    def schema_registry_url(self) -> str | None:
        return self._backend.schema_registry_url

    def fresh_topic(self) -> str:
        topic = f"td_it_{uuid.uuid4().hex[:12]}"
        _create_topic(self._backend, topic)
        return topic

    def produce_json(self, topic: str, messages: list[dict]) -> None:
        _produce_json(self._backend, topic, messages)

    def delete_topic(self, topic: str) -> None:
        _delete_topic(self._backend, topic)


@pytest.fixture
def kafka():
    """Factory building a ``_Kafka`` broker client over the redpanda backend the
    test declared -- call it with ``domain_redpanda``."""
    return _Kafka
