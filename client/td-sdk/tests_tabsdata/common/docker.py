#
# Copyright 2026 Tabs Data Inc.
#

"""Docker utility functions."""

import logging
import os

import docker

from tabsdata._utils.constants import TRUE_VALUES

# Import after container/system to avoid circular dependency
from tests_tabsdata.common.backend import (
    DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
    DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
)
from tests_tabsdata.common.container import get_container_port_binding
from tests_tabsdata.common.system import get_free_port

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
            DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
            DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
        ]
        for prefix in ["MYSQL0", "MARIADB0", "POSTGRES0", "ORACLE0"]:
            # Check for {prefix}__MYSQL_DOCKER_CONTAINER_NAME environment variables
            mysql_env_var = f"{prefix}__MYSQL_DOCKER_CONTAINER_NAME"
            if (
                os.environ.get(f"{prefix}__MYSQL_DONT_KILL") not in TRUE_VALUES
                and os.environ.get(f"{prefix}__MYSQL_ENVS") not in TRUE_VALUES
            ):
                mysql_container_name = os.environ.get(
                    mysql_env_var,
                    DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
                )
                docker_containers_to_clean.append(mysql_container_name)
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
