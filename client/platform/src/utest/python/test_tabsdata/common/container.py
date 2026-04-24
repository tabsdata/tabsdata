#
# Copyright 2026 Tabs Data Inc.
#

"""Docker containers utility functions."""

import logging
from time import sleep

from test_tabsdata.common.constants import (
    CONTAINER_WAIT_POLL_INTERVAL_SECONDS,
    CONTAINER_WAIT_TIMEOUT_SECONDS,
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


def get_container_port_binding(container, port: str) -> int:
    port_binding = container.attrs["NetworkSettings"]["Ports"].get(port)
    if port_binding and len(port_binding) > 0:
        return int(port_binding[0]["HostPort"])
    else:
        raise Exception(f"Port {port} is not bound in the container.")
