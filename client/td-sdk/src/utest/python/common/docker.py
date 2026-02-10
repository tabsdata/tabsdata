#
# Copyright 2026 Tabs Data Inc.
#

"""Docker utility functions."""

import logging
import os

import docker

from tabsdata._utils.constants import TRUE_VALUES

# Import after container/system to avoid circular dependency
from src.utest.python.common.backend import (
    DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
)
from src.utest.python.common.container import get_container_port_binding
from src.utest.python.common.system import get_free_port

logger = logging.getLogger(__name__)

__all__ = [
    "get_free_port",
    "get_container_port_binding",
    "clean_docker_containers",
    "remove_docker_containers",
]


def clean_docker_containers():
    try:
        docker_containers_to_clean = [
            DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
        ]
        for prefix in ["MYSQL0", "MARIADB0", "POSTGRES0", "ORACLE0", "KAFKA0"]:
            # Check for {prefix}__<BACKEND>_DOCKER_CONTAINER_NAME environment variables
            backend_type = prefix.rstrip("0")
            env_var = f"{prefix}__{backend_type}_DOCKER_CONTAINER_NAME"
            if (
                os.environ.get(f"{prefix}__{backend_type}_DONT_KILL") not in TRUE_VALUES
                and os.environ.get(f"{prefix}__{backend_type}_ENVS") not in TRUE_VALUES
            ):
                container_name = os.environ.get(env_var)
                if container_name:
                    docker_containers_to_clean.append(container_name)
                elif "MYSQL" in backend_type:
                    docker_containers_to_clean.append(
                        DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME
                    )
        name_pattern = "^(" + "|".join(docker_containers_to_clean) + ")$"
        remove_docker_containers(name_pattern)
    except Exception as e:
        logger.warning(
            "Error removing Docker containers. You can safely ignore it if running"
            f" tests on a Docker-less machine: {e}"
        )


def remove_docker_containers(name_pattern):
    # noinspection PyPackageRequirements

    client = docker.from_env()
    for container in client.containers.list(all=True, filters={"name": name_pattern}):
        try:
            logger.info(f"Removing container {container}")
            container.remove(v=True, force=True)
        except Exception as e:
            logger.error(f"Failed to remove container {container}. Error: {e}")
