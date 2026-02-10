#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.stage_trigger import LocalFileStageTriggerContext
from tabsdata.connector.kafka._connector import AwsGlueSchemaRegistry, ConfluentSchemaRegistry
from tabsdata.connector.kafka.serdes.serde import KafkaSerde
from tabsdata.connector.kafka.typing import AwsGlueSchemaRegistryCredentialsSpec

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdata.common.backend import KafkaConfig
from test_tabsdata_kafka.bootest import TESTING_RESOURCES_PATH

TIMEOUT_SECS = 32


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    kafka_config,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)

pytest_plugins = ["test_tabsdata.fntest"]


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


import json
import os
import shutil
import uuid
from unittest.mock import MagicMock

import avro.schema
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
    MockSchemaRegistryClient,
)
from confluent_kafka.serialization import MessageField, SerializationContext
from mypy_boto3_glue.client import GlueClient

from tabsdata.connector.kafka.serdes.aws_glue_sr_serde import (
    AvroGlueKafkaSerde,
    GlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)
from tabsdata.connector.kafka.serdes.confluent_sr_serde import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
)
from tabsdata.connector.kafka.serdes.non_sr_serde import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    ProtobufKafkaSerde,
)


class MockLocalFileStageTriggerContext(LocalFileStageTriggerContext):

    def trigger(self) -> bool:
        return True

    def trigger_monitor(self) -> bool:
        return True


# Non Schema Registry
@pytest.fixture(scope="module")
def avro_serde_v1():
    """Fixture for the original Avro schema (v1)."""
    schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user_v1.avsc")
    schema = open(schema_path, "r").read()
    return AvroKafkaSerde(schema)


@pytest.fixture(scope="module")
def avro_serde_v2():
    """Fixture for the updated Avro schema (v2), which is backward compatible."""
    schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user_v2.avsc")
    schema = open(schema_path, "r").read()
    return AvroKafkaSerde(schema)


PERSON_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0},
        "city": {"type": "string"},
    },
    "required": ["name", "age"],
    "additionalProperties": False,
}


@pytest.fixture
def json_serde():
    """
    Fixture to provide an instance of JsonKafkaSerde with a schema.
    """
    return JsonKafkaSerde(json_schema=json.dumps(PERSON_SCHEMA))


@pytest.fixture
def json_serde_no_schema():
    """
    Fixture to provide an instance of JsonKafkaSerde without a schema.
    """
    return JsonKafkaSerde(json_schema=None)


PROTOBUF_MESSAGE_NAME = "PersonV1"


@pytest.fixture(scope="module")
def protobuf_serde_v1(tmp_path_factory):
    """Fixture for the original Protobuf schema (v1)."""
    working_dir = tmp_path_factory.mktemp("protobuf_gen_v1")
    proto_file_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "person_v1.proto")
    proto_schema = open(proto_file_path, "r").read()
    message_name = "PersonV1"
    serde = ProtobufKafkaSerde(proto_schema, message_name, str(working_dir))
    yield serde
    shutil.rmtree(working_dir)


@pytest.fixture(scope="module")
def protobuf_serde_v2(tmp_path_factory):
    """Fixture for the updated Protobuf schema (v2)."""
    working_dir = tmp_path_factory.mktemp("protobuf_gen_v2")
    proto_file_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "person_v2.proto")
    proto_schema = open(proto_file_path, "r").read()
    message_name = "PersonV2"
    serde = ProtobufKafkaSerde(proto_schema, message_name, str(working_dir))
    yield serde
    shutil.rmtree(working_dir)


# Confluent Schema Registry


@pytest.fixture(scope="function")
def confluent_sr_mock_registry():
    """
    Fixture to provide a mock schema registry client, ensuring it is reset for each test.
    """
    # The MockSchemaRegistryClient is a singleton, so we need to reset it for each test.
    MockSchemaRegistryClient._instance = None
    return MockSchemaRegistryClient({"url": "http://mock-url"})


@pytest.fixture
def confluent_schema_registry():
    def _sr(config: KafkaConfig, _serde: KafkaSerde):
        return ConfluentSchemaRegistry(
            url=config.schema_registry_url,
            credentials=UserPasswordCredentials(
                user=config.credentials.user.secret_value,
                password=config.credentials.password.secret_value,
            ),
            configs=None,
        )

    return _sr


@pytest.fixture(scope="function")
def confluent_sr_avro_serde(
    confluent_sr_mock_registry, kafka_schema_registry, monkeypatch
):
    """
    Fixture to provide an AvroSrKafkaSerde instance with a mock schema registry.
    """

    def _factory(config: KafkaConfig, mock=False):
        schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user_v1.avsc")
        with open(schema_path, "r") as f:
            schema_str = f.read()
        # The Avro Deserializer needs a parsed schema, not just a string
        parsed_schema = avro.schema.parse(schema_str)
        sr_client = (
            kafka_schema_registry(config) if not mock else confluent_sr_mock_registry
        )
        return AvroSrKafkaSerde(sr_client, schema_str)

    return _factory


@pytest.fixture(scope="function")
def confluent_sr_json_serde(confluent_sr_mock_registry, kafka_schema_registry):
    """
    Fixture to provide a JsonSrKafkaSerde instance with a mock schema registry.
    """

    def _factory(config: KafkaConfig, mock=False):
        schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user.json")
        with open(schema_path, "r") as f:
            schema_str = f.read()
        schema = Schema(schema_str, schema_type="JSON")
        sr_client = (
            kafka_schema_registry(config) if not mock else confluent_sr_mock_registry
        )
        return JsonSrKafkaSerde(
            schema_registry_client=sr_client, json_schema=schema_str
        )

    return _factory


@pytest.fixture(scope="function")
def confluent_sr_protobuf_serde(
    confluent_sr_mock_registry,
    kafka_schema_registry,
    tmp_path_factory,
):
    """
    Fixture to provide a ProtobufSrKafkaSerde instance with a mock schema registry.
    """

    def _factory(config: KafkaConfig = None, mock=False):
        working_dir = tmp_path_factory.mktemp("protobuf_gen")

        proto_file_path = os.path.join(
            TESTING_RESOURCES_PATH, "schemas", "person_v1.proto"
        )
        proto_schema = open(proto_file_path, "r").read()

        message_name = "PersonV1"
        sr_client = (
            kafka_schema_registry(config) if not mock else confluent_sr_mock_registry
        )

        return ProtobufSrKafkaSerde(
            schema_registry_client=sr_client,
            protobuf_schema=proto_schema,
            message_name=message_name,
            working_dir=str(working_dir),
        )

    return _factory


# AWS Glue Schema Registry


@pytest.fixture
def aws_glue_schema_registry():
    def _sr(_config: KafkaConfig, serde: GlueKafkaSerde):
        registry_name = getattr(serde, "_registry_name")
        schema_name = getattr(serde, "_schema_name")
        return AwsGlueSchemaRegistry(
            region_name="us-east-1",
            credentials=AwsGlueSchemaRegistryCredentialsSpec(
                aws_access_key_id="test_access_key_id",
                aws_secret_access_key="test_secret_access_key",
            ),
            registry_name=registry_name,
            schema_name=schema_name,
            configs=None,
        )

    return _sr


@pytest.fixture(scope="module")
def aws_glue_sr_json_schema_str():
    schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user.json")
    with open(schema_path, "r") as f:
        return f.read()


@pytest.fixture
def aws_glue_sr_mock_json_glue_client(aws_glue_sr_json_schema_str):
    mock_client = MagicMock(spec=GlueClient)
    schema_version_id = str(uuid.uuid4())

    mock_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": schema_version_id
    }
    mock_client.get_schema_version.return_value = {
        "SchemaDefinition": aws_glue_sr_json_schema_str
    }
    return mock_client, schema_version_id


@pytest.fixture
def aws_glue_sr_json_glue_serde(
    aws_glue_sr_json_schema_str, aws_glue_sr_mock_json_glue_client, monkeypatch
):
    mock_client, _ = aws_glue_sr_mock_json_glue_client
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: mock_client)
    return JsonGlueKafkaSerde(
        glue_client=mock_client,
        registry_name="test-registry",
        schema_name="user-schema",
        json_schema=aws_glue_sr_json_schema_str,
        validate_schema=True,
    )


@pytest.fixture(scope="module")
def aws_glue_sr_avro_schema_str():
    schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "user_v1.avsc")
    with open(schema_path, "r") as f:
        return f.read()


@pytest.fixture
def aws_glue_sr_mock_avro_glue_client(aws_glue_sr_avro_schema_str):
    mock_client = MagicMock(spec=GlueClient)
    schema_version_id = str(uuid.uuid4())

    mock_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": schema_version_id
    }
    mock_client.get_schema_version.return_value = {
        "SchemaDefinition": aws_glue_sr_avro_schema_str
    }
    return mock_client, schema_version_id


@pytest.fixture
def aws_glue_sr_avro_glue_serde(
    aws_glue_sr_avro_schema_str,
    aws_glue_sr_mock_avro_glue_client,
    monkeypatch,
):
    mock_client, _ = aws_glue_sr_mock_avro_glue_client
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: mock_client)
    return AvroGlueKafkaSerde(
        glue_client=mock_client,
        registry_name="test-registry",
        schema_name="user-schema",
        avro_schema=aws_glue_sr_avro_schema_str,
    )


@pytest.fixture(scope="module")
def aws_glue_sr_protobuf_schema_str():
    schema_path = os.path.join(TESTING_RESOURCES_PATH, "schemas", "person_v1.proto")
    with open(schema_path, "r") as f:
        return f.read()


@pytest.fixture
def aws_glue_sr_mock_protobuf_glue_client(aws_glue_sr_protobuf_schema_str):
    mock_client = MagicMock(spec=GlueClient)
    schema_version_id = str(uuid.uuid4())

    mock_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": schema_version_id
    }
    mock_client.get_schema_version.return_value = {
        "SchemaDefinition": aws_glue_sr_protobuf_schema_str
    }
    return mock_client, schema_version_id


@pytest.fixture
def aws_glue_sr_protobuf_glue_serde(
    aws_glue_sr_protobuf_schema_str,
    aws_glue_sr_mock_protobuf_glue_client,
    tmp_path_factory,
    monkeypatch,
):
    mock_client, _ = aws_glue_sr_mock_protobuf_glue_client
    working_dir = tmp_path_factory.mktemp("protobuf_gen")
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: mock_client)
    return ProtobufGlueKafkaSerde(
        glue_client=mock_client,
        registry_name="test-registry",
        schema_name="person-schema",
        protobuf_schema=aws_glue_sr_protobuf_schema_str,
        message_name="PersonV1",
        working_dir=str(working_dir),
    )


# Kafka Fixtures


@pytest.fixture
def kafka_topic_creator(kafka_config: KafkaConfig):
    admin_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }

    def topic_creator(topic_name: str):
        admin_client = AdminClient(admin_config)
        admin_client.list_topics(timeout=TIMEOUT_SECS)
        topic = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]

        topics = admin_client.list_topics(timeout=TIMEOUT_SECS)

        fs = admin_client.create_topics(topic)

        topics = admin_client.list_topics(timeout=TIMEOUT_SECS)
        for topic, f in fs.items():
            try:
                f.result(timeout=TIMEOUT_SECS)
                logger.debug(f"Topic '{topic}' created")
            except Exception as e:
                error_message = str(e)
                if "TOPIC_ALREADY_EXISTS" in error_message:
                    logger.debug(f"Topic '{topic}' already exists. Moving on...")
                else:
                    logger.debug(f"Failed to create topic {topic}: {e}")
                    raise

    return topic_creator


@pytest.fixture
def kafka_producer(kafka_config: KafkaConfig) -> Producer:
    producer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }
    producer = Producer(producer_config)
    topics = producer.list_topics(timeout=TIMEOUT_SECS)

    return producer


@pytest.fixture
def kafka_consumer_fixture(kafka_config: KafkaConfig) -> Consumer:
    config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }
    consumer = Consumer(config)
    consumer.list_topics(timeout=TIMEOUT_SECS)
    return consumer


@pytest.fixture
def kafka_schema_registry():
    def _sr_client(config: KafkaConfig):
        sr_client = SchemaRegistryClient(
            {
                "url": config.schema_registry_url,
                "basic.auth.user.info": f"{config.user}:{config.password}",
                "timeout": TIMEOUT_SECS,  # milliseconds
            }
        )
        return sr_client

    return _sr_client


@pytest.fixture
def produce_kafka_messages(kafka_topic_creator, kafka_producer):
    def _produce(topic, sent_messages, serde, get_message):
        kafka_topic_creator(topic)
        ctx = SerializationContext(topic, MessageField.VALUE)
        for i in range(1, sent_messages + 1):
            message = get_message(i)
            kafka_producer.produce(
                topic,
                value=serde.serializer()(message, ctx),
                callback=lambda err, msg: logger.debug(
                    f"Produced message to {msg.topic()}"
                ),
            )
            kafka_producer.list_topics(timeout=TIMEOUT_SECS)
            kafka_producer.poll(0)
            kafka_producer.list_topics(timeout=TIMEOUT_SECS)
        kafka_producer.flush()
        kafka_producer.list_topics(timeout=TIMEOUT_SECS)

    return _produce


def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")


def get_avro_message(i: int):
    return {"name": f"name_{i}", "favorite_number": i, "favorite_color": "blue"}


def get_json_message(i: int):
    return {"name": f"name_{i}", "age": i, "city": "some city"}


def get_protobuf_message(i: int):
    return {"name": f"name_{i}", "id": i, "email": f"name_{i}@provider.com"}


def get_large_avro_message(i: int):
    return {"name": f"name_{i}", "favorite_number": i, "favorite_color": "a" * 1024}


def get_large_json_message(i: int):
    return {"name": f"name_{i}", "age": i, "city": "a" * 1024}


def get_large_protobuf_message(i: int):
    return {
        "name": f"name_{i}" + "a" * 1024,
        "id": i,
        "email": f"name_{i}@provider.com",
    }
