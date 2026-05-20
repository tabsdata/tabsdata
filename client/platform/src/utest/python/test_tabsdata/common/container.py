#
# Copyright 2026 Tabs Data Inc.
#

"""Docker containers utility functions."""

import logging
from time import sleep

from test_tabsdata.common.constants import (
    CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS,
    CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS,
    CONTAINER_WAIT_POLL_INTERVAL_SECONDS,
    CONTAINER_WAIT_TIMEOUT_SECONDS,
    IMAGE_PULL_MAXIMUM_RETRY_COUNT,
    IMAGE_PULL_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


def wait_for_container_running(container):
    elapsed = 0
    while container.status != "running":
        if elapsed >= CONTAINER_WAIT_TIMEOUT_SECONDS:
            raise Exception(
                f"Container {container.name} did not reach 'running' state "
                f"after {CONTAINER_WAIT_TIMEOUT_SECONDS} seconds "
                f"(current status: {container.status})"
            )
        logger.info(
            f"Waiting for container {container.name} to be running "
            f"(status: {container.status}); retrying in "
            f"{CONTAINER_WAIT_POLL_INTERVAL_SECONDS} second(s)..."
        )
        sleep(CONTAINER_WAIT_POLL_INTERVAL_SECONDS)
        elapsed += CONTAINER_WAIT_POLL_INTERVAL_SECONDS
        container.reload()


def _container_port(container, port: str) -> int | None:
    port_binding = (
        container.attrs.get("NetworkSettings", {}).get("Ports", {}).get(port)
    )
    if not port_binding:
        return None
    host_port = port_binding[0].get("HostPort")
    if not host_port:
        return None
    try:
        return int(host_port)
    except (TypeError, ValueError):
        return None


def _wait_for_container_port(container, port: str) -> int | None:
    elapsed = 0.0
    while elapsed <= CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS:
        container.reload()
        host_port = _container_port(container, port)
        if host_port is not None:
            return host_port
        sleep(CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS)
        elapsed += CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS
    return None


def get_container_port_binding(container, port: str) -> int:
    host_port = _wait_for_container_port(container, port)
    if host_port is None:
        raise Exception(f"Port {port} is not bound in the container.")
    return host_port


def pull_docker_image(image, platform=None):
    import docker
    from tenacity import retry, stop_after_attempt, wait_fixed

    client = docker.from_env(timeout=IMAGE_PULL_TIMEOUT_SECONDS)
    pull_kwargs = {}
    if platform is not None:
        pull_kwargs["platform"] = platform

    @retry(
        stop=stop_after_attempt(IMAGE_PULL_MAXIMUM_RETRY_COUNT),
        wait=wait_fixed(30),
        reraise=True,
    )
    def try_pull():
        logger.info(f"Pulling Docker image '{image}'...")
        client.images.pull(image, **pull_kwargs)
        logger.info(f"Successfully pulled Docker image '{image}'")

    try:
        try_pull()
    except Exception as err:
        logger.error(
            f"Failed to pull Docker image '{image}' "
            f"after {IMAGE_PULL_MAXIMUM_RETRY_COUNT} attempts: {err}"
        )
        raise


def pull_compose_images(compose_file):
    import yaml

    with open(compose_file) as f:
        compose = yaml.safe_load(f)

    images = set()
    for service in compose.get("services", {}).values():
        image = service.get("image")
        if image:
            images.add(image)

    for image in sorted(images):
        pull_docker_image(image)
