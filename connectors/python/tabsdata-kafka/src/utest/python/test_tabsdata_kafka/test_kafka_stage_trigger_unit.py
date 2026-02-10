#
# Copyright 2026 Tabs Data Inc.
#

import os
import uuid
from typing import cast
from unittest.mock import patch

import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValidationError,
)
from tabsdata_kafka import KafkaMessageFormatSpec
from tabsdata_kafka._connector import (
    AwsGlueSchemaRegistry,
    AwsGlueSchemaRegistryCredentialsSpec,
    ConfluentSchemaRegistry,
    KafkaConn,
    KafkaTrigger,
)
from tabsdata_kafka.serdes.aws_glue_sr_serde import (
    AvroGlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)
from tabsdata_kafka.serdes.confluent_sr_serde import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
)
from tabsdata_kafka.serdes.non_sr_serde import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    ProtobufKafkaSerde,
)
from tabsdata_kafka.typing import Protobuf
from test_tabsdata_kafka.conftest import (
    aws_glue_sr_mock_protobuf_glue_client,
)


@pytest.fixture
def kafka_consumer_config() -> dict:
    config = {
        "bootstrap.servers": os.environ.get("KF0__KAFKA_SERVERS", "localhost:19092"),
        "group.id": os.environ.get("KF0__KAFKA_GROUP_ID", str(uuid.uuid4())),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": os.environ.get("KF0__KAFKA_SASL_USERNAME", "admin"),
        "sasl.password": os.environ.get("KF0__KAFKA_SASL_PASSWORD", "secret"),
    }
    return config


@pytest.fixture
def confluent_schema_registry():
    return ConfluentSchemaRegistry(
        url="http://mock-url",
        credentials=UserPasswordCredentials(
            user="test_user",
            password="test_password",
        ),
        configs=None,
    )


@pytest.fixture
def aws_glue_schema_registry():
    return AwsGlueSchemaRegistry(
        region_name="us-east-1",
        credentials=AwsGlueSchemaRegistryCredentialsSpec(
            aws_access_key_id="test_access_key_id",
            aws_secret_access_key="test_secret_access_key",
        ),
        registry_name="registry-name",
        schema_name="schema-name",
        configs=None,
    )


@pytest.fixture
def kafka_conn():
    # Use explicit params to match KafkaConn signature
    return KafkaConn(
        servers="localhost:9092",
        credentials=UserPasswordCredentials(
            user="test_user",
            password="test_password",
        ),  # or provide a UserPasswordCredentials if needed
        group_id="test-group",
        schema_registry=None,  # Set in tests as needed
        enforce_connection_params=True,
        cx_src_configs=None,
    )


@pytest.fixture
def kafka_conn_confluent_sr(confluent_schema_registry):
    # Use explicit params to match KafkaConn signature
    return KafkaConn(
        servers="localhost:9092",
        credentials=UserPasswordCredentials(
            user="test_user",
            password="test_password",
        ),  # or provide a UserPasswordCredentials if needed
        group_id="test-group",
        schema_registry=confluent_schema_registry,  # Set in tests as needed
        enforce_connection_params=True,
        cx_src_configs=None,
    )


@pytest.fixture
def kafka_conn_aws_glue_sr(aws_glue_schema_registry):
    # Use explicit params to match KafkaConn signature
    return KafkaConn(
        servers="localhost:9092",
        credentials=UserPasswordCredentials(
            user="test_user",
            password="test_password",
        ),  # or provide a UserPasswordCredentials if needed
        group_id="test-group",
        schema_registry=aws_glue_schema_registry,  # Set in tests as needed
        enforce_connection_params=True,
        cx_src_configs=None,
    )


@pytest.fixture
def mock_boto3_glue_client(aws_glue_sr_mock_protobuf_glue_client):
    mock_client, schema_version_id = aws_glue_sr_mock_protobuf_glue_client
    with patch("boto3.client", return_value=mock_client):
        yield mock_client, schema_version_id


def test_confluent_schema_registry_properties():
    creds = UserPasswordCredentials(user="user", password="pass")
    configs = {"foo": "bar"}
    sr = ConfluentSchemaRegistry(
        url="http://localhost:8081",
        credentials=creds,
        configs=configs,
    )
    assert sr.url == "http://localhost:8081"
    assert sr.credentials == creds
    assert sr.configs == configs
    assert "ConfluentSchemaRegistry" in repr(sr)


def test_aws_glue_schema_registry_properties():
    creds = AwsGlueSchemaRegistryCredentialsSpec(
        aws_access_key_id="AWS_ACCESS_KEY_ID...",
        aws_secret_access_key="AWS_SECRET_ACCESS_KEY...",
    )
    configs = {"endpoint_url": "http://localhost:4566"}
    sr = AwsGlueSchemaRegistry(
        region_name="us-west-2",
        credentials=creds,
        configs=configs,
        registry_name="registry-name",
        schema_name="schema-name",
    )
    assert sr.region_name == "us-west-2"
    assert sr.credentials == creds
    assert sr.configs == configs
    assert "AwsGlueSchemaRegistry" in repr(sr)
    assert sr.registry_name == "registry-name"
    assert sr.schema_name == "schema-name"


def test_kafka_conn_properties(confluent_schema_registry):
    servers = ["localhost:9092", "localhost:9093"]
    creds = UserPasswordCredentials(user="user", password="pass")
    group_id = "test-group"
    cx_src_configs = {"foo": "bar"}
    conn = KafkaConn(
        servers=servers,
        credentials=creds,
        group_id=group_id,
        schema_registry=confluent_schema_registry,
        enforce_connection_params=False,
        cx_src_configs=cx_src_configs,
    )
    assert conn.servers == servers
    assert conn.credentials == creds
    assert conn.group_id == group_id
    assert conn.schema_registry == confluent_schema_registry
    assert conn.enforce_connection_params is False
    assert conn.cx_src_configs == cx_src_configs


def test_kafka_conn_servers_str(confluent_schema_registry):
    servers = "localhost:9092"
    conn = KafkaConn(
        servers=servers,
        credentials=UserPasswordCredentials("u", "p"),
        group_id=None,
        schema_registry=confluent_schema_registry,
        enforce_connection_params=True,
        cx_src_configs=None,
    )
    assert conn.servers == servers
    assert conn.schema_registry == confluent_schema_registry


@pytest.mark.unit
@pytest.mark.kafka
def test_properties_minimal(kafka_conn):
    trigger = KafkaTrigger(
        conn=kafka_conn,
        topic="test-topic",
        data_format="json",
        schema="{}",
    )
    assert trigger.conn == kafka_conn
    assert trigger.topic == "test-topic"
    assert trigger.data_format == "json"
    assert trigger.schema == "{}"
    assert trigger.group_id is None
    assert trigger.time_rollover_secs == 60
    assert trigger.size_rollover_mb is None
    assert trigger.messages_rollover is None
    assert trigger.dlq_topic is None


@pytest.mark.unit
@pytest.mark.kafka
def test_properties_full(kafka_conn):
    trigger = KafkaTrigger(
        conn=kafka_conn,
        topic="topic-x",
        data_format="avro",
        schema="{avro_schema}",
        group_id="group-x",
        time_rollover_secs=120,
        size_rollover_mb=10,
        messages_rollover=100,
        dlq_topic="dlq-topic-x",
    )
    assert trigger.conn == kafka_conn
    assert trigger.topic == "topic-x"
    assert trigger.data_format == "avro"
    assert trigger.schema == "{avro_schema}"
    assert trigger.group_id == "group-x"
    assert trigger.time_rollover_secs == 120
    assert trigger.size_rollover_mb == 10
    assert trigger.messages_rollover == 100
    assert trigger.dlq_topic == "dlq-topic-x"


@pytest.mark.unit
@pytest.mark.kafka
def test_type_validation(kafka_conn):
    # Should raise if time_rollover_secs is not positive int
    with pytest.raises(ParameterValidationError) as e:
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            time_rollover_secs=-1,
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if topic is not a string
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        KafkaTrigger(
            conn=kafka_conn,
            topic=123,
            data_format="json",
            schema="{}",
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if data_format is not a valid KafkaMessageFormatSpec or string
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format=123,
            schema="{}",
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if schema is not a string
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema=123,
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if size_rollover_mb is not a SizeRollover or None
    with pytest.raises(ParameterValidationError) as e:
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            size_rollover_mb=-2,
        )
    assert e.value.error_code == ErrorCode.STCE103

    with pytest.raises(ParameterValidationError) as e:
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            size_rollover_mb=-1,
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if messages_rollover is not a MessagesRollover or None
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            messages_rollover="a",
        )
    assert e.value.error_code == ErrorCode.STCE103

    with pytest.raises(ParameterValidationError) as e:
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            messages_rollover=-1,
        )
    assert e.value.error_code == ErrorCode.STCE103

    # Should raise if dlq_topic is not a string or None
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        KafkaTrigger(
            conn=kafka_conn,
            topic="t",
            data_format="json",
            schema="{}",
            dlq_topic=123,
        )
    assert e.value.error_code == ErrorCode.STCE103


@pytest.mark.parametrize(
    "conn_fixture, data_format, schema_fixture, expected_serde_class",
    [
        (
            "kafka_conn_aws_glue_sr",
            "avro",
            "aws_glue_sr_avro_schema_str",
            AvroGlueKafkaSerde,
        ),
        (
            "kafka_conn_aws_glue_sr",
            "json",
            "aws_glue_sr_json_schema_str",
            JsonGlueKafkaSerde,
        ),
        (
            "kafka_conn_aws_glue_sr",
            "protobuf",
            "aws_glue_sr_protobuf_schema_str",
            ProtobufGlueKafkaSerde,
        ),
        (
            "kafka_conn_confluent_sr",
            "avro",
            "aws_glue_sr_avro_schema_str",
            AvroSrKafkaSerde,
        ),
        (
            "kafka_conn_confluent_sr",
            "json",
            "aws_glue_sr_json_schema_str",
            JsonSrKafkaSerde,
        ),
        (
            "kafka_conn_confluent_sr",
            "protobuf",
            "aws_glue_sr_protobuf_schema_str",
            ProtobufSrKafkaSerde,
        ),
        (
            "kafka_conn",
            "avro",
            "aws_glue_sr_avro_schema_str",
            AvroKafkaSerde,
        ),
        (
            "kafka_conn",
            "json",
            "aws_glue_sr_json_schema_str",
            JsonKafkaSerde,
        ),
        (
            "kafka_conn",
            "protobuf",
            "aws_glue_sr_protobuf_schema_str",
            ProtobufKafkaSerde,
        ),
    ],
)
@pytest.mark.unit
@pytest.mark.kafka
def test_serde_factory(
    conn_fixture,
    data_format,
    schema_fixture,
    expected_serde_class,
    kafka_conn,
    tmp_path,
    request,
    mock_boto3_glue_client,
):
    conn = request.getfixturevalue(conn_fixture) if conn_fixture is not None else None
    schema = request.getfixturevalue(schema_fixture)
    mock_client, _ = mock_boto3_glue_client

    if data_format == "protobuf":
        data_format = Protobuf("PersonV1")

    trigger = KafkaTrigger(
        conn=conn,
        topic="topic-x",
        data_format=cast(KafkaMessageFormatSpec, data_format),
        schema=schema,
    )

    tr_sr = trigger._get_serde(str(tmp_path))
    assert isinstance(tr_sr, expected_serde_class)
