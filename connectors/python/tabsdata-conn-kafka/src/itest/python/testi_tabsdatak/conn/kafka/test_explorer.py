#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka connection-explorer integration tests"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from testi_tabsdatak.conn.kafka.backend import broker, credentials, group_id, src_conn

from tabsdatak.conn.kafka import KafkaStreamSrcConn
from tabsdatak.conn.kafka._explorer import KafkaConnectionExplorer, KafkaTopicEntity
from tabsdatak.conn.kafka.error import KafkaRuntimeException

pytestmark = pytest.mark.kafka


def _entity_by_topic(entities: list[KafkaTopicEntity], topic: str) -> KafkaTopicEntity:
    matches = [e for e in entities if e.topic == topic]
    assert matches, f"no entity for {topic!r} in {[e.topic for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@redpanda_domain("redpanda")
def test_check_connect_succeeds(domain_redpanda: RedpandaDomain):
    conn = src_conn(domain_redpanda)
    assert KafkaConnectionExplorer().check_connect(conn) is None


@redpanda_domain("redpanda")
def test_check_connect_unreachable_broker_raises(domain_redpanda: RedpandaDomain):
    unreachable = KafkaStreamSrcConn(
        servers=f"{domain_redpanda.host}:1",
        credentials=credentials(domain_redpanda),
        group_id=group_id(),
    )
    with pytest.raises(KafkaRuntimeException):
        KafkaConnectionExplorer().check_connect(unreachable)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@redpanda_domain("redpanda")
def test_metadata_lists_created_topic(domain_redpanda: RedpandaDomain):
    domain = domain_redpanda
    conn = src_conn(domain)
    with broker(domain).fresh_topic() as topic:
        entities = KafkaConnectionExplorer().metadata(conn)

        assert entities and all(isinstance(e, KafkaTopicEntity) for e in entities)
        created = _entity_by_topic(entities, topic)
        assert created.partitions == 1
        assert created.fqn() == topic

    # Kafka topics carry no columnar schema at the connection level.
    assert created.field_names() == []


@redpanda_domain("redpanda")
def test_metadata_skips_internal_topics(domain_redpanda: RedpandaDomain):
    conn = src_conn(domain_redpanda)
    entities = KafkaConnectionExplorer().metadata(conn)
    assert not [e for e in entities if e.topic.startswith("__")]


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@redpanda_domain("redpanda")
def test_data_preview_unsupported(domain_redpanda: RedpandaDomain):
    domain = domain_redpanda
    conn = src_conn(domain)
    explorer = KafkaConnectionExplorer()
    with broker(domain).fresh_topic() as topic:
        entity = _entity_by_topic(explorer.metadata(conn), topic)
        # A message payload is only decodable with the source's serde, so the
        # explorer inherits the base's "no row preview".
        assert explorer.data(conn, entity) is None
