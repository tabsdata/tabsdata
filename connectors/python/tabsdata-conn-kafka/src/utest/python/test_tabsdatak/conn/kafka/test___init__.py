#
# Copyright 2026 Tabsdata Inc.
#

"""Public SR / Protobuf types on the kafka connector -- shape +
validation. ``KafkaStreamSrcConn`` / ``KafkaStreamSrc`` coverage lands
alongside the connector module in a follow-up commit."""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.kafka import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    Protobuf,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka.error import KafkaErrorCode


pytestmark = pytest.mark.kafka


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _aws_creds() -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret("AKIA"),
        secret_access_key=Secret("SECRET"),
    )


# --- Protobuf -------------------------------------------------------


def test_protobuf_round_trip():
    p = Protobuf(message_name="com.example.UserEvent")
    assert p.message_name == "com.example.UserEvent"


def test_protobuf_rejects_non_string():
    with pytest.raises(ConnInitException) as ei:
        Protobuf(message_name=42)  # type: ignore[arg-type]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_4


# --- ConfluentSchemaRegistry ----------------------------------------


def test_confluent_sr_minimal():
    sr = ConfluentSchemaRegistry(url="http://sr:8081")
    assert sr.url == "http://sr:8081"
    assert sr.credentials is None
    assert sr.configs is None


def test_confluent_sr_with_credentials():
    sr = ConfluentSchemaRegistry(url="http://sr", credentials=_creds())
    assert sr.credentials is not None


def test_confluent_sr_with_configs():
    sr = ConfluentSchemaRegistry(
        url="http://sr",
        configs={"cache.capacity": 100},
    )
    assert sr.configs == {"cache.capacity": 100}


def test_confluent_sr_rejects_missing_url():
    with pytest.raises(ConnInitException) as ei:
        ConfluentSchemaRegistry()  # type: ignore[call-arg]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_2


# --- AwsGlueSchemaRegistry ------------------------------------------


def test_aws_glue_sr_minimal():
    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1",
        credentials=_aws_creds(),
        registry_name="reg",
        schema_name="sch",
    )
    assert sr.region_name == "us-east-1"
    assert sr.registry_name == "reg"
    assert sr.schema_name == "sch"


def test_aws_glue_sr_rejects_missing_credentials():
    with pytest.raises(ConnInitException) as ei:
        AwsGlueSchemaRegistry(  # type: ignore[call-arg]
            region_name="us-east-1",
            registry_name="reg",
            schema_name="sch",
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_3


def test_aws_glue_sr_rejects_missing_registry_name():
    with pytest.raises(ConnInitException) as ei:
        AwsGlueSchemaRegistry(  # type: ignore[call-arg]
            region_name="us-east-1",
            credentials=_aws_creds(),
            schema_name="sch",
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_3


# --- KafkaStreamSrcConn ------------------------------------------------------


def test_kafka_conn_minimal_construction():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    conn = KafkaStreamSrcConn(servers="broker:9092", group_id="g1")
    assert conn.servers == "broker:9092"
    assert conn.credentials is None
    assert conn.group_id == "g1"
    assert conn.schema_registry is None
    assert conn.conn_cfg is None


def test_kafka_conn_with_credentials_and_group():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    conn = KafkaStreamSrcConn(
        servers="broker:9092",
        credentials=_creds(),
        group_id="my-group",
    )
    assert conn.credentials is not None
    assert conn.group_id == "my-group"


def test_kafka_conn_accepts_confluent_sr():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    sr = ConfluentSchemaRegistry(url="http://sr")
    conn = KafkaStreamSrcConn(servers="b:9092", group_id="g1", schema_registry=sr)
    assert isinstance(conn.schema_registry, ConfluentSchemaRegistry)


def test_kafka_conn_accepts_glue_sr():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1",
        credentials=_aws_creds(),
        registry_name="reg",
        schema_name="sch",
    )
    conn = KafkaStreamSrcConn(servers="b:9092", group_id="g1", schema_registry=sr)
    assert isinstance(conn.schema_registry, AwsGlueSchemaRegistry)


def test_kafka_conn_rejects_missing_servers():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    with pytest.raises(ConnInitException) as ei:
        KafkaStreamSrcConn(group_id="g1")  # type: ignore[call-arg]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_1


def test_kafka_conn_rejects_missing_group_id():
    from tabsdatak.conn.kafka import KafkaStreamSrcConn

    with pytest.raises(ConnInitException) as ei:
        KafkaStreamSrcConn(servers="broker:9092")  # type: ignore[call-arg]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_1


# --- KafkaStreamSrc -------------------------------------------------


def test_kafka_stream_src_minimal_construction():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        topic="events",
        data_format="json",
        schema="",
    )
    assert src.topic == "events"
    assert src.data_format == "json"
    assert src.dlq_topic is None


def test_kafka_stream_src_output_table_is_topic():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        topic="events",
        data_format="json",
        schema="",
    )
    assert src.output_table() == "events"


def test_kafka_stream_src_accepts_protobuf_format():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        topic="events",
        data_format=Protobuf(message_name="com.example.Foo"),
        schema='syntax = "proto3"; message Foo {}',
    )
    assert isinstance(src.data_format, Protobuf)


def test_kafka_stream_src_rejects_unknown_src_cfg_key():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    with pytest.raises(ConnInitException) as ei:
        KafkaStreamSrc(
            topic="events",
            data_format="json",
            schema="",
            src_cfg={"bogus.key": 1},  # type: ignore[dict-item]
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_5


def test_kafka_stream_src_accepts_known_src_cfg_keys():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        topic="events",
        data_format="json",
        schema="",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "tabsdata.kafka.buffer.max_rows": 100,
            "tabsdata.kafka.buffer.max_bytes": 1024,
            "tabsdata.kafka.buffer.max_secs": 5,
            "tabsdata.kafka.trigger.max_secs": 60,
            "tabsdata.kafka.trigger.max_bytes": 16,
            "tabsdata.kafka.trigger.max_rows": 1000,
            "kafka.logging_level": 20,
        },
    )
    assert src.src_cfg["tabsdata.kafka.trigger.max_bytes"] == 16


def test_kafka_stream_src_json_round_trip():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        topic="events",
        data_format="json",
        schema='{"type": "object"}',
        dlq_topic="events_dlq",
        src_cfg={"tabsdata.kafka.trigger.max_rows": 500},
    )
    loaded = KafkaStreamSrc.from_json(src.to_json())
    assert loaded == src


def test_kafka_stream_src_json_round_trip_keeps_a_marked_trigger_max_secs():
    from tabsdatak.conn.common._stream._floor import unsafe_seconds
    from tabsdatak.conn.kafka import KafkaStreamSrc

    marked = unsafe_seconds(1)
    src = KafkaStreamSrc(
        topic="events",
        data_format="json",
        schema="",
        src_cfg={"tabsdata.kafka.trigger.max_secs": marked},
    )
    loaded = KafkaStreamSrc.from_json(src.to_json())
    assert loaded.src_cfg["tabsdata.kafka.trigger.max_secs"] == marked


@pytest.mark.parametrize(
    "key",
    [
        "kafka.time_rollover_seconds",
        "kafka.messages_rollover",
        "kafka.size_rollover_mb",
    ],
)
def test_kafka_stream_src_rejects_the_removed_rollover_keys(key):
    from tabsdatak.conn.kafka import KafkaStreamSrc

    with pytest.raises(ConnInitException):
        KafkaStreamSrc(topic="events", data_format="json", schema="", src_cfg={key: 60})


@pytest.mark.parametrize(
    "key",
    [
        "tabsdata.kafka.buffer.max_rows",
        "tabsdata.kafka.buffer.max_bytes",
        "tabsdata.kafka.buffer.max_secs",
        "tabsdata.kafka.trigger.max_rows",
        "tabsdata.kafka.trigger.max_bytes",
    ],
)
@pytest.mark.parametrize("value", [0, -1, 2.5])
def test_kafka_stream_src_rejects_a_limit_that_is_not_positive(key, value):
    from tabsdatak.conn.kafka import KafkaStreamSrc

    with pytest.raises(ConnInitException, match="must be a positive whole number"):
        KafkaStreamSrc(
            topic="events", data_format="json", schema="", src_cfg={key: value}
        )
