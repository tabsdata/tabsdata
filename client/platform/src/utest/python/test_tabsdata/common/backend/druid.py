#
# Copyright 2026 Tabs Data Inc.
#

"""Apache Druid fixtures and utilities.

Druid is a multi-process system — a working cluster needs ZooKeeper,
a metadata store (PostgreSQL), and several Druid services.  The compose
file (``druid-compose.yaml``) declares the full stack; this module uses
``docker compose`` to start it and the Docker SDK for container queries
and port bindings, following the same pattern as the other backend
fixtures.
"""

import logging
import os
import subprocess
from dataclasses import dataclass
from time import sleep

import pytest
from filelock import FileLock

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    CONTAINER_MAXIMUM_RETRY_COUNT,
    DB_HOST,
    DOCKER_COMPOSE_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_DRUID_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_COMPOSE_CONTAINER_INFIX}druid"
)

# The coordinator service acts as the Overlord in single-node mode.
DRUID_OVERLORD_PORT = 8081
DRUID_OVERLORD_FORWARDED_PORT = "8081/tcp"
DRUID_ROUTER_PORT = 8888
DRUID_ROUTER_FORWARDED_PORT = "8888/tcp"

VALID_DRUID_PREFIXES = ["DRUID0"]

COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "druid-compose.yaml")


@dataclass
class DruidConfig:
    host: str
    overlord_port: int
    router_port: int
    overlord_url: str
    router_url: str
    credentials: UserPasswordCredentials | None


@pytest.fixture(scope="function")
def druid_config(tmp_path_factory, worker_id, request) -> DruidConfig:
    marker = request.node.get_closest_marker("druid")
    prefix = marker.kwargs.get("env_prefix", "DRUID0") if marker else "DRUID0"
    if prefix not in VALID_DRUID_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_DRUID_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_DRUID_PREFIXES' variable in "
            "the 'druid.py' file."
        )
    if os.environ.get(f"{prefix}__DRUID_ENVS") in TRUE_VALUES:
        overlord_url = os.environ.get(f"{prefix}__DRUID_OVERLORD_URL")
        if not overlord_url:
            raise Exception(
                f"The environment variable {prefix}__DRUID_OVERLORD_URL is not set. "
                "Unable to run tests using the 'druid_config' fixture."
            )
        router_url = os.environ.get(f"{prefix}__DRUID_ROUTER_URL", "")
        user = os.environ.get(f"{prefix}__DRUID_USER")
        password = os.environ.get(f"{prefix}__DRUID_PASSWORD")
        credentials = (
            UserPasswordCredentials(user, password)
            if user and password
            else None
        )
        return DruidConfig(
            host=DB_HOST,
            overlord_port=DRUID_OVERLORD_PORT,
            router_port=DRUID_ROUTER_PORT,
            overlord_url=overlord_url,
            router_url=router_url,
            credentials=credentials,
        )
    else:
        name = os.environ.get(
            f"{prefix}__DRUID_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_DRUID_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            overlord_port, router_port = create_docker_druid(name)
        else:
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            fn = root_tmp_dir / "docker_druid_creation"
            with FileLock(str(fn) + ".lock"):
                overlord_port, router_port = create_docker_druid(name)
        return DruidConfig(
            host=DB_HOST,
            overlord_port=overlord_port,
            router_port=router_port,
            overlord_url=f"http://{DB_HOST}:{overlord_port}",
            router_url=f"http://{DB_HOST}:{router_port}",
            credentials=None,
        )


def _get_compose_container(client, project_name: str, service: str):
    """Get a Docker container for a compose service by project and service name."""
    containers = client.containers.list(
        filters={
            "label": [
                f"com.docker.compose.project={project_name}",
                f"com.docker.compose.service={service}",
            ]
        }
    )
    return containers[0] if containers else None


def create_docker_druid(
    name: str = DEFAULT_PYTEST_DRUID_DOCKER_CONTAINER_NAME,
) -> tuple[int, int]:
    """Start the Druid cluster via docker-compose and wait for readiness.

    Uses ``docker compose -p <name>`` to start the stack declared in
    ``druid-compose.yaml``.  If the stack is already running, reuses it.
    Container queries and port bindings go through the Docker SDK.

    Args:
        name: Compose project name (used with ``docker compose -p``).

    Returns:
        Tuple of (overlord_host_port, router_host_port).
    """
    import docker  # Lazy import

    logger.info("Starting Druid cluster")
    client = docker.from_env()

    # Check if the compose stack is already running
    coordinator = _get_compose_container(client, name, "coordinator")
    if coordinator:
        logger.info("Druid cluster already running")
        overlord_port = get_container_port_binding(
            coordinator, DRUID_OVERLORD_FORWARDED_PORT
        )
        router = _get_compose_container(client, name, "router")
        router_port = get_container_port_binding(
            router, DRUID_ROUTER_FORWARDED_PORT
        )
        return overlord_port, router_port

    # Start the full stack — compose handles health checks, dependencies,
    # networking, and volumes declaratively.
    subprocess.run(
        ["docker", "compose", "-p", name, "-f", COMPOSE_FILE, "up", "-d"],
        check=True,
    )

    # Fetch port bindings via the Docker SDK
    coordinator = _get_compose_container(client, name, "coordinator")
    overlord_port = get_container_port_binding(
        coordinator, DRUID_OVERLORD_FORWARDED_PORT
    )
    router = _get_compose_container(client, name, "router")
    router_port = get_container_port_binding(
        router, DRUID_ROUTER_FORWARDED_PORT
    )

    # Wait for the Overlord API to be ready
    import requests  # Lazy import

    retry = 0
    while True:
        try:
            resp = requests.get(
                f"http://{DB_HOST}:{overlord_port}/status",
                timeout=5,
            )
            resp.raise_for_status()
            logger.info("Druid Overlord is ready")
            break
        except Exception as err:
            retry += 1
            if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
                logger.error(f"Error connecting to Druid Overlord: {err}")
                raise err
            else:
                logger.warning(
                    f"Error connecting to Druid Overlord, "
                    f"retrying in {retry} second(s)"
                )
                sleep(retry)

    logger.info("Druid cluster created successfully")
    return overlord_port, router_port
