#
#  Copyright 2026 Tabs Data Inc.
#

"""Kafka/Redpanda fixtures and utilities."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from time import sleep

import pytest
import requests
from filelock import FileLock

from tabsdata._credentials import Credentials, UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from tests_tabsdata.common.constants import (
    MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from tests_tabsdata.common.container import get_container_port_binding
from tests_tabsdata.common.system import get_free_port

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}c_kafka"
)

KAFKA_INTERNAL_LISTENER_PORT = 9092
KAFKA_EXTERNAL_LISTENER_PORT = 19092
KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT = 8081
KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT = 18081
KAFKA_ADMIN_INTERNAL_PORT = 9644
KAFKA_ADMIN_EXTERNAL_PORT = 19644
KAFKA_RPC_INTERNAL_PORT = 33145

KAFKA_BIND_ADDRESS = "0.0.0.0"
KAFKA_ADVERTISE_ADDRESS = "localhost"

KAFKA_ADMIN_USER = "admin"
KAFKA_ADMIN_PASSWORD = "secret"

VALID_KAFKA_PREFIXES = ["KAFKA0"]


@dataclass
class KafkaConfig:
    host: str
    kafka_port: int
    schema_registry_port: int
    admin_port: int
    user: str
    password: str
    bootstrap_servers: str
    schema_registry_url: str
    admin_url: str
    credentials: Credentials


@pytest.fixture(scope="function")
def kafka_config(tmp_path_factory, worker_id, request) -> KafkaConfig:
    marker = request.node.get_closest_marker("kafka")
    prefix = marker.kwargs.get("env_prefix", "KAFKA0") if marker else "KAFKA0"
    if prefix not in VALID_KAFKA_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_KAFKA_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_KAFKA_PREFIXES' variable in "
            "the 'kafka.py' file."
        )

    if os.environ.get(f"{prefix}__KAFKA_ENVS") in TRUE_VALUES:
        host = os.environ.get(f"{prefix}__KAFKA_HOST")
        if not host:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_HOST is not set. Unable to "
                "run tests using the 'kafka_config' fixture."
            )
        kafka_port = os.environ.get(f"{prefix}__KAFKA_PORT")
        if not kafka_port:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_PORT is not set. Unable to "
                "run tests using the 'kafka_config' fixture."
            )
        schema_registry_port = os.environ.get(f"{prefix}__KAFKA_SCHEMA_REGISTRY_PORT")
        if not schema_registry_port:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_SCHEMA_REGISTRY_PORT is not "
                "set. Unable to run tests using the 'kafka_config' fixture."
            )
        admin_port = os.environ.get(f"{prefix}__KAFKA_ADMIN_PORT")
        if not admin_port:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_ADMIN_PORT is not set. "
                "Unable to run tests using the 'kafka_config' fixture."
            )
        user = os.environ.get(f"{prefix}__KAFKA_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_USER is not set. Unable to "
                "run tests using the 'kafka_config' fixture."
            )
        password = os.environ.get(f"{prefix}__KAFKA_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__KAFKA_PASSWORD is not set. "
                "Unable to run tests using the 'kafka_config' fixture."
            )

        config = KafkaConfig(
            host=host,
            kafka_port=int(kafka_port),
            schema_registry_port=int(schema_registry_port),
            admin_port=int(admin_port),
            user=user,
            password=password,
            bootstrap_servers=f"{host}:{kafka_port}",
            schema_registry_url=f"http://{host}:{schema_registry_port}",
            admin_url=f"http://{host}:{admin_port}",
            credentials=UserPasswordCredentials(user, password),
        )
        return config
    else:
        name = os.environ.get(
            f"{prefix}__KAFKA_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
        )
        ports = None
        if worker_id == "master":
            # not executing with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            ports = create_docker_kafka_container(name)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / "docker_kafka_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the container
                ports = create_docker_kafka_container(name)

        kafka_port, schema_registry_port, admin_port = ports
        host = KAFKA_ADVERTISE_ADDRESS

        config = KafkaConfig(
            host=host,
            kafka_port=kafka_port,
            schema_registry_port=schema_registry_port,
            admin_port=admin_port,
            user=KAFKA_ADMIN_USER,
            password=KAFKA_ADMIN_PASSWORD,
            bootstrap_servers=f"{host}:{kafka_port}",
            schema_registry_url=f"http://{host}:{schema_registry_port}",
            admin_url=f"http://{host}:{admin_port}",
            credentials=UserPasswordCredentials(KAFKA_ADMIN_USER, KAFKA_ADMIN_PASSWORD),
        )
        return config


def create_docker_kafka_container(
    name: str = DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
) -> tuple[int, int, int]:
    # noinspection PyPackageRequirements
    import docker  # Lazy import

    logger.info("Starting Kafka/Redpanda container")
    client = docker.from_env()

    if containers := client.containers.list(filters={"name": name}):
        logger.info("Kafka/Redpanda container already exists")
        container = containers[0]
        # Extract ports from container labels
        labels = container.labels
        kafka_port = int(labels.get("kafka_port", 0))
        schema_registry_port = int(labels.get("schema_registry_port", 0))
        admin_port = int(labels.get("admin_port", 0))

        if not (kafka_port and schema_registry_port and admin_port):
            raise Exception(
                f"Container {name} exists but ports are not properly labeled. "
                "Please remove the container and try again."
            )

        logger.info(
            f"Using existing container ports - Kafka: {kafka_port}, "
            f"Schema Registry: {schema_registry_port}, Admin: {admin_port}"
        )
        return kafka_port, schema_registry_port, admin_port
    else:
        # Get random free ports for external (host) access
        # These will map to standard internal ports (9092, 8081, 9644)
        kafka_port = get_free_port()
        schema_registry_port = get_free_port()
        admin_port = get_free_port()

        logger.info(
            f"Selected external ports - Kafka: {kafka_port}, "
            f"Schema Registry: {schema_registry_port}, Admin: {admin_port}"
        )

        # Load the kafka.sh startup script template
        script_path = Path(__file__).parent / "kafka.sh"
        with open(script_path, "r") as f:
            startup_script = f.read()

        # Replace placeholders with constant values and dynamic ports
        startup_command = startup_script.replace(
            "${KAFKA_BIND_ADDRESS}",
            KAFKA_BIND_ADDRESS,
        )
        startup_command = startup_command.replace(
            "${KAFKA_ADVERTISE_ADDRESS}",
            KAFKA_ADVERTISE_ADDRESS,
        )
        startup_command = startup_command.replace(
            "${KAFKA_INTERNAL_LISTENER_PORT}",
            str(KAFKA_INTERNAL_LISTENER_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_EXTERNAL_LISTENER_PORT}",
            str(KAFKA_EXTERNAL_LISTENER_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_EXTERNAL_HOST_PORT}",
            str(kafka_port),
        )
        startup_command = startup_command.replace(
            "${KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT}",
            str(KAFKA_SCHEMA_REGISTRY_INTERNAL_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT}",
            str(KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_SCHEMA_REGISTRY_HOST_PORT}",
            str(schema_registry_port),
        )
        startup_command = startup_command.replace(
            "${KAFKA_ADMIN_INTERNAL_PORT}",
            str(KAFKA_ADMIN_INTERNAL_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_ADMIN_EXTERNAL_PORT}",
            str(KAFKA_ADMIN_EXTERNAL_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_RPC_INTERNAL_PORT}",
            str(KAFKA_RPC_INTERNAL_PORT),
        )
        startup_command = startup_command.replace(
            "${KAFKA_ADMIN_USER}",
            KAFKA_ADMIN_USER,
        )
        startup_command = startup_command.replace(
            "${KAFKA_ADMIN_PASSWORD}",
            KAFKA_ADMIN_PASSWORD,
        )

        startup_command = "\n".join(line.lstrip() for line in startup_command.splitlines())

        container = client.containers.run(
            "redpandadata/redpanda:latest",
            name=name,
            entrypoint="/bin/sh",
            command=["-c", startup_command],
            ports={
                f"{KAFKA_EXTERNAL_LISTENER_PORT}/tcp": kafka_port,
                f"{KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT}/tcp": schema_registry_port,
                f"{KAFKA_ADMIN_EXTERNAL_PORT}/tcp": admin_port,
            },
            labels={
                "kafka_port": str(kafka_port),
                "schema_registry_port": str(schema_registry_port),
                "admin_port": str(admin_port),
            },
            detach=True,
            remove=False,
        )

        container.reload()
        actual_kafka_port = get_container_port_binding(
            container, f"{KAFKA_EXTERNAL_LISTENER_PORT}/tcp"
        )
        actual_schema_port = get_container_port_binding(
            container, f"{KAFKA_SCHEMA_REGISTRY_EXTERNAL_PORT}/tcp"
        )
        actual_admin_port = get_container_port_binding(
            container, f"{KAFKA_ADMIN_EXTERNAL_PORT}/tcp"
        )

        assert (
            actual_kafka_port == kafka_port
        ), f"Kafka port mismatch: expected {kafka_port}, got {actual_kafka_port}"
        assert actual_schema_port == schema_registry_port, (
            f"Schema Registry port mismatch: expected {schema_registry_port}, got"
            f" {actual_schema_port}"
        )
        assert (
            actual_admin_port == admin_port
        ), f"Admin port mismatch: expected {admin_port}, got {actual_admin_port}"

        logger.info("Waiting for Redpanda to be ready...")
        retry = 0
        ready_marker = "[td fixture] Redpanda is Ready!"
        while True:
            try:
                logs = container.logs(tail=100).decode("utf-8")
                if ready_marker in logs:
                    logger.info("Redpanda startup script completed")
                    break

                retry += 1
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error("Timeout waiting for Redpanda to start")
                    logger.error(f"Container logs:\n{logs}")
                    raise Exception("Timeout waiting for Redpanda to start")
                else:
                    logger.info(
                        f"Waiting for Redpanda to be ready; retrying in {retry} "
                        "second(s)..."
                    )
                    sleep(retry)
            except Exception as error:
                retry += 1
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error waiting for Redpanda: {error}")
                    raise error
                else:
                    logger.warning(
                        f"Error checking Redpanda status; retrying in {retry} second(s)"
                    )
                    sleep(retry)

        logger.info("Verifying cluster health...")
        retry = 0
        while True:
            try:
                health_check = container.exec_run(
                    "rpk cluster health --api-urls"
                    f" {KAFKA_BIND_ADDRESS}:{KAFKA_ADMIN_INTERNAL_PORT}"
                )
                if health_check.exit_code == 0:
                    logger.info("Redpanda cluster health check passed")
                    break
                else:
                    logger.warning(
                        f"Health check failed with exit code {health_check.exit_code}: "
                        f"{health_check.output.decode('utf-8')}"
                    )
                    retry += 1
                    if retry == MAXIMUM_RETRY_COUNT:
                        raise Exception(
                            f"Cluster health check failed after {MAXIMUM_RETRY_COUNT}"
                            " retries"
                        )
                    sleep(retry)
            except Exception as error:
                retry += 1
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error running health check: {error}")
                    raise error
                else:
                    logger.warning(
                        f"Error running health check; retrying in {retry} second(s)"
                    )
                    sleep(retry)

        logger.info("Verifying Schema Registry...")
        retry = 0
        while True:
            try:
                response = requests.get(
                    f"http://{KAFKA_ADVERTISE_ADDRESS}:{schema_registry_port}/subjects",
                    timeout=5,
                    auth=(KAFKA_ADMIN_USER, KAFKA_ADMIN_PASSWORD),
                )
                if response.status_code == 200:
                    logger.info(f"Schema Registry check passed: {response.text}")
                    break
                else:
                    logger.warning(
                        f"Schema Registry returned status {response.status_code}: "
                        f"{response.text}"
                    )
                    retry += 1
                    if retry == MAXIMUM_RETRY_COUNT:
                        raise Exception(
                            f"Schema Registry check failed after {MAXIMUM_RETRY_COUNT}"
                            " retries"
                        )
                    sleep(retry)
            except Exception as error:
                retry += 1
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error checking Schema Registry: {error}")
                    raise error
                else:
                    logger.warning(
                        f"Error checking Schema Registry; retrying in {retry} second(s)"
                    )
                    sleep(retry)

        logger.info("Verifying Kafka SASL-enabled API...")
        retry = 0
        while True:
            try:
                from confluent_kafka.admin import AdminClient

                test_admin_config = {
                    "bootstrap.servers": f"{KAFKA_ADVERTISE_ADDRESS}:{kafka_port}",
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanism": "SCRAM-SHA-256",
                    "sasl.username": KAFKA_ADMIN_USER,
                    "sasl.password": KAFKA_ADMIN_PASSWORD,
                }
                test_admin = AdminClient(test_admin_config)
                test_admin.list_topics(timeout=5)
                logger.info("Kafka SASL API check passed")
                break
            except Exception as error:
                retry += 1
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error checking Kafka SASL API: {error}")
                    raise error
                else:
                    logger.warning(
                        f"Error checking Kafka SASL API; retrying in {retry} second(s)"
                    )
                    sleep(retry)

        logger.info("Kafka/Redpanda container created successfully")
        return kafka_port, schema_registry_port, admin_port
