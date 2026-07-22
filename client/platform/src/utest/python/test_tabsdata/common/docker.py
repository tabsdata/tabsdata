#
# Copyright 2026 Tabsdata Inc.
#

"""Docker utility functions."""

import logging
import os

import docker

from tabsdata._utils.constants import TRUE_VALUES

from test_tabsdata.common.constants import PYTEST_DEFAULT_ENVIRONMENT_PREFIX
from test_tabsdata.common.container import get_container_port_binding
from test_tabsdata.common.system import get_free_port

logger = logging.getLogger(__name__)

TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"

__all__ = [
    "get_free_port",
    "get_container_port_binding",
    "clean_docker_containers",
    "remove_docker_containers",
    "remove_docker_volumes",
]


def clean_docker_containers():
    try:
        remove_docker_containers(f"^{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}")
        remove_docker_volumes(PYTEST_DEFAULT_ENVIRONMENT_PREFIX)
        for prefix in ["CLICKHOUSE0",
                       "DB20",
                       "DB2CDC0",
                       "DRUID0",
                       "HASHICORP",
                       "H1",
                       "KAFKA0",
                       "MARIADB0",
                       "MSSSQL0",
                       "MYSQL0",
                       "MYSQLCDC0",
                       "ORACLE0",
                       "POSTGRES0",
                       "POSTGRESCDC0",
                       "STARROCKS0",]:
            backend_type = prefix.rstrip("0")
            env_var = f"{prefix}__{backend_type}_DOCKER_CONTAINER_NAME"
            if (
                os.environ.get(f"{prefix}__{backend_type}_DONT_KILL") not in TRUE_VALUES
                and os.environ.get(f"{prefix}__{backend_type}_ENVS") not in TRUE_VALUES
            ):
                container_name = os.environ.get(env_var)
                if container_name:
                    remove_docker_containers(f"^{container_name}$")
    except Exception as error:
        logger.warning(
            f"Error removing Docker containers. "
            f"You can safely ignore it "
            f"if running tests on a Docker-less machine: {error}"
        )


def remove_docker_containers(name_pattern):
    if os.environ.get(TD_DOCKER_DONT_KILL) in TRUE_VALUES:
        logger.warning("fSkipping removal of test Docker container '{name_pattern}'.")
        return

    client = docker.from_env()
    for container in client.containers.list(all=True, filters={"name": name_pattern}):
        try:
            logger.info(f"Removing container {container}")
            container.remove(v=True, force=True)
        except Exception as error:
            logger.error(
                f"Failed to remove container {container}. "
                f"Error: {error}"
            )


def remove_docker_volumes(name_prefix):
    client = docker.from_env()
    for volume in client.volumes.list():
        if volume.name.startswith(name_prefix):
            try:
                logger.info(f"Removing volume {volume.name}")
                volume.remove(force=True)
            except Exception as error:
                logger.error(
                    f"Failed to remove volume {volume.name}. "
                    f"Error: {error}"
                )
