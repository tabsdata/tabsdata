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
