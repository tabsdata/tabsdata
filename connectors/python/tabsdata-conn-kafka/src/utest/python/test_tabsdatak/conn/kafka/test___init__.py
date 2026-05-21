"""Public SR / Protobuf types on the kafka connector -- shape +
validation. ``KafkaConn`` / ``KafkaStreamSrc`` coverage lands
alongside the connector module in a follow-up commit."""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    Protobuf,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka.error import KafkaErrorCode


pytestmark = pytest.mark.kafka


def _creds() -> UserPassword:
    return UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))


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
        credentials=_creds(),
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
            credentials=_creds(),
            schema_name="sch",
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_3


# --- KafkaConn ------------------------------------------------------


def _kafka_conn_clss():
    from tabsdatak.conn.kafka import KafkaConn  # late-imported so

    return KafkaConn  # this file works before commit 3 too


def test_kafka_conn_minimal_construction():
    from tabsdatak.conn.kafka import KafkaConn

    conn = KafkaConn(servers="broker:9092")
    assert conn.servers == "broker:9092"
    assert conn.credentials is None
    assert conn.group_id is None
    assert conn.schema_registry is None
    assert conn.enforce_connection_params is True
    assert conn.conn_cfg is None


def test_kafka_conn_accepts_servers_list():
    from tabsdatak.conn.kafka import KafkaConn

    conn = KafkaConn(servers=["a:9092", "b:9092"])
    assert conn.servers == ["a:9092", "b:9092"]


def test_kafka_conn_with_credentials_and_group():
    from tabsdatak.conn.kafka import KafkaConn

    conn = KafkaConn(
        servers="broker:9092",
        credentials=_creds(),
        group_id="my-group",
    )
    assert conn.credentials is not None
    assert conn.group_id == "my-group"


def test_kafka_conn_accepts_confluent_sr():
    from tabsdatak.conn.kafka import KafkaConn

    sr = ConfluentSchemaRegistry(url="http://sr")
    conn = KafkaConn(servers="b:9092", schema_registry=sr)
    assert isinstance(conn.schema_registry, ConfluentSchemaRegistry)


def test_kafka_conn_accepts_glue_sr():
    from tabsdatak.conn.kafka import KafkaConn

    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1",
        credentials=_creds(),
        registry_name="reg",
        schema_name="sch",
    )
    conn = KafkaConn(servers="b:9092", schema_registry=sr)
    assert isinstance(conn.schema_registry, AwsGlueSchemaRegistry)


def test_kafka_conn_rejects_missing_servers():
    from tabsdatak.conn.kafka import KafkaConn

    with pytest.raises(ConnInitException) as ei:
        KafkaConn()  # type: ignore[call-arg]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_1


# --- KafkaStreamSrc -------------------------------------------------


def _kafka_conn():
    from tabsdatak.conn.kafka import KafkaConn

    return KafkaConn(servers="broker:9092", credentials=_creds())


def test_kafka_stream_src_minimal_construction():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format="json",
        schema="",
    )
    assert src.topic == "events"
    assert src.data_format == "json"
    assert src.group_id is None
    assert src.table is None
    assert src.dlq_topic is None
    assert src.start is None
    assert src.end is None


def test_kafka_stream_src_output_table_defaults_to_topic():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(), topic="events", data_format="json", schema="",
    )
    assert src.output_table() == "events"


def test_kafka_stream_src_output_table_explicit_override():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format="json",
        schema="",
        table="custom_table",
    )
    assert src.output_table() == "custom_table"


def test_kafka_stream_src_accepts_protobuf_format():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format=Protobuf(message_name="com.example.Foo"),
        schema='syntax = "proto3"; message Foo {}',
    )
    assert isinstance(src.data_format, Protobuf)


def test_kafka_stream_src_accepts_start_end_window():
    from datetime import datetime, timezone

    from tabsdatak.conn.kafka import KafkaStreamSrc

    start = datetime(2026, 5, 18, tzinfo=timezone.utc)
    end = datetime(2026, 5, 19, tzinfo=timezone.utc)
    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format="json",
        schema="",
        start=start,
        end=end,
    )
    assert src.start == start
    assert src.end == end


def test_kafka_stream_src_rejects_unknown_src_cfg_key():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    with pytest.raises(ConnInitException) as ei:
        KafkaStreamSrc(
            conn=_kafka_conn(),
            topic="events",
            data_format="json",
            schema="",
            src_cfg={"bogus.key": 1},  # type: ignore[dict-item]
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_5


def test_kafka_stream_src_accepts_known_src_cfg_keys():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format="json",
        schema="",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "kafka.time_rollover_seconds": 30,
            "kafka.size_rollover_mb": 16,
            "kafka.messages_rollover": 1000,
            "kafka.logging_level": 20,
        },
    )
    assert src.src_cfg["kafka.size_rollover_mb"] == 16


def test_kafka_stream_src_json_round_trip():
    from tabsdatak.conn.kafka import KafkaStreamSrc

    src = KafkaStreamSrc(
        conn=_kafka_conn(),
        topic="events",
        data_format="json",
        schema='{"type": "object"}',
        group_id="g1",
        table="evt_tbl",
        dlq_topic="events_dlq",
        src_cfg={"kafka.messages_rollover": 500},
    )
    loaded = KafkaStreamSrc.from_json(src.to_json())
    assert loaded == src
