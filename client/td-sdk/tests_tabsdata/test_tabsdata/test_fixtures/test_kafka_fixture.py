#
# Copyright 2026 Tabs Data Inc.
#

import logging

import pytest
import requests
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.kafka
def test_kafka_fixture_basic(kafka_config):
    assert kafka_config.host is not None
    assert kafka_config.kafka_port is not None
    assert kafka_config.schema_registry_port is not None
    assert kafka_config.admin_port is not None
    assert kafka_config.user is not None
    assert kafka_config.password is not None

    assert kafka_config.bootstrap_servers.startswith(kafka_config.host)
    assert str(kafka_config.kafka_port) in kafka_config.bootstrap_servers

    assert kafka_config.schema_registry_url.startswith("http://")
    assert kafka_config.schema_registry_url.endswith(
        str(kafka_config.schema_registry_port)
    )

    assert kafka_config.admin_url.startswith("http://")
    assert kafka_config.admin_url.endswith(str(kafka_config.admin_port))

    assert kafka_config.credentials.user.secret_value == kafka_config.user
    assert kafka_config.credentials.password.secret_value == kafka_config.password

    print(f"\nKafka Config:")
    print(f"  Bootstrap Servers: {kafka_config.bootstrap_servers}")
    print(f"  Schema Registry: {kafka_config.schema_registry_url}")
    print(f"  Admin API: {kafka_config.admin_url}")
    print(f"  User: {kafka_config.user}")


@pytest.mark.kafka
def test_kafka_connection(kafka_config):
    admin_client = AdminClient(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": kafka_config.user,
            "sasl.password": kafka_config.password,
            "session.timeout.ms": 10000,
            "socket.timeout.ms": 10000,
        }
    )

    metadata = admin_client.list_topics(timeout=10)
    assert metadata.brokers, "AdminClient metadata did not return any brokers"
    assert kafka_config.bootstrap_servers.endswith(
        str(kafka_config.kafka_port)
    ), "Bootstrap servers should include external Kafka port"

    print(f"\nSuccessfully connected to Kafka via confluent-kafka AdminClient!")
    print(f"  Brokers: {list(metadata.brokers.keys())}")


@pytest.mark.kafka
def test_kafka_authentication_required(kafka_config):
    bad_client = AdminClient(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": "wrong-user",
            "sasl.password": "wrong-pass",
            "session.timeout.ms": 5000,
            "socket.timeout.ms": 5000,
        }
    )

    with pytest.raises(KafkaException) as excinfo:
        bad_client.list_topics(timeout=5)

    error = excinfo.value.args[0]
    assert hasattr(error, "code"), "KafkaException did not return a KafkaError"
    assert error.code() in {
        KafkaError._AUTHENTICATION,
        KafkaError.SASL_AUTHENTICATION_FAILED,
        KafkaError._TRANSPORT,
    }, f"Unexpected error code for failed auth: {error.code()}"


@pytest.mark.kafka
def test_schema_registry_connection(kafka_config):
    response = requests.get(
        f"{kafka_config.schema_registry_url}/subjects",
        auth=(kafka_config.user, kafka_config.password),
    )

    assert response.status_code == 200, (
        f"Failed to connect to Schema Registry: {response.status_code}\n"
        f"Response: {response.text}"
    )

    subjects = response.json()
    print(f"\nSchema Registry connected successfully!")
    print(f"  Available subjects: {subjects}")


@pytest.mark.kafka
def test_schema_registry_requires_auth(kafka_config):
    no_auth = requests.get(f"{kafka_config.schema_registry_url}/subjects")
    if no_auth.status_code == 200:
        pytest.skip("Schema Registry is running without authentication")
    assert no_auth.status_code in {
        401,
        403,
    }, "Schema Registry should reject requests without authentication"

    wrong_auth = requests.get(
        f"{kafka_config.schema_registry_url}/subjects",
        auth=("wrong-user", "wrong-pass"),
    )
    assert wrong_auth.status_code in {
        401,
        403,
    }, "Schema Registry should reject requests with invalid credentials"
