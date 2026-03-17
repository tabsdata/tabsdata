#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import Any, Callable
import pytest
import docker
from docker.models.containers import Container

from tenacity import retry, stop_after_attempt, wait_incrementing

from test_tabsdata.common.constants import (
    CONTAINER_MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX, DB_PASSWORD, DOCKER_REGULAR_CONTAINER_INFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

PYTEST_XDIST_MASTER: str = "master"

DOCKER_LOCK_SUFFIX: str = ".lock"

ENVIRONMENT_PREFIX: str = "env_prefix"

CDC_CONTAINER_PREFIX: str = f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}"

CDC_DATABASE: str = "tdcdcdb"
CDC_USER: str = "tdcdcuser"
CDC_PASSWORD: str = DB_PASSWORD

def ensure_container(
    name: str,
    image: str,
    environment: list[str],
    forwarded_ports: dict[str, None],
    command: list[str] | None = None,
    entrypoint: str | None = None,
    platform: str | None = None,
    privileged: bool = False,
    remove: bool = False,
    message: str | None = None,
) -> tuple[Container, int, bool]:
    client = docker.from_env()
    forwarded_port_key = next(iter(forwarded_ports))

    if containers := client.containers.list(filters={"name": f"^{name}$"}):
        logger.info(f"CDC container '{name}' already exists; reusing it.")
        container = containers[0]
        host_port = get_container_port_binding(container, forwarded_port_key)
        return container, host_port, False

    logger.info(f"Creating CDC container '{name}' from image '{image}'")
    run_kwargs: dict[str, Any] = {
        "name": name,
        "environment": environment,
        "ports": forwarded_ports,
        "detach": True,
        "privileged": privileged,
        "remove": remove,
    }
    if command is not None:
        run_kwargs["command"] = command
    if entrypoint is not None:
        run_kwargs["entrypoint"] = entrypoint
    if platform is not None:
        run_kwargs["platform"] = platform
    container = client.containers.run(image, **run_kwargs)
    container.reload()
    if message is not None:
        ensure_readiness(container, message)
    host_port = get_container_port_binding(container, forwarded_port_key)
    return container, host_port, True


def ensure_connection(
    function: Callable[[], None],
    database: str,
    retries: int = CONTAINER_MAXIMUM_RETRY_COUNT,
) -> None:
    @retry(
        stop=stop_after_attempt(retries),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def try_connect() -> None:
        logger.info(f"Attempting to connect to database '{database}'...")
        function()
        logger.info(f"Successfully connected to database '{database}'")
    try:
        try_connect()
    except Exception as exception:
        logger.error(f"Failed to connect to database '{database}'"
                     f" after {retries} attempts: {exception}")
        raise


def ensure_readiness(
    container: Container,
    message: str,
    retries: int = CONTAINER_MAXIMUM_RETRY_COUNT,
) -> None:
    @retry(
        stop=stop_after_attempt(retries),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def try_read() -> None:
        logger.info(f"Checking for message '{message}' "
                    f"in container '{container.name}' logs...")
        logs = container.logs().decode("utf-8", errors="replace")
        if message not in logs:
            raise RuntimeError(f"The message '{message}' "
                               f"is not yet in container '{container.name}' logs")
        logger.info(f"Found message '{message}' in container '{container.name}' logs")

    try:
        try_read()
    except Exception as exception:
        logger.error(f"Failed to find message '{message}' "
                     f"in container '{container.name}' "
                     f"logs after {retries} attempts: {exception}"
        )
        raise


def obtain_prefix(
    fixture_request: pytest.FixtureRequest,
    marker_name: str,
    default_prefix: str,
    valid_prefixes: list[str],
) -> str:
    marker = fixture_request.node.get_closest_marker(marker_name)
    prefix = marker.kwargs.get(ENVIRONMENT_PREFIX, default_prefix) if marker else default_prefix
    if prefix not in valid_prefixes:
        raise Exception(
            f"The prefix '{prefix}' is not valid. "
            f"Valid prefixes are: {', '.join(valid_prefixes)}. "
            f"If you want to extend the list of valid prefixes, "
            f"please modify the corresponding 'VALID_*_PREFIXES' "
            f"variable in the corresponding pytest setup file."
        )
    return prefix


def obtain_environment(
    env_prefix: str,
    keys: list[str],
    fixture_name: str,
) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in keys:
        env_name = f"{env_prefix}_{key}"
        value = os.environ.get(env_name)
        if not value:
            raise Exception(
                f"The environment variable {env_name} is not set."
                f" Unable to run tests using the '{fixture_name}' fixture."
            )
        values[key] = value
    return values
