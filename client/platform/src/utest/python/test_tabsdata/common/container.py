#
# Copyright 2026 Tabs Data Inc.
#

"""Docker containers utility functions."""

import logging
from time import sleep

from test_tabsdata.common.constants import CONTAINER_MAXIMUM_RETRY_COUNT

logger = logging.getLogger(__name__)


def wait_for_container_running(container):
    retry = 0
    while container.status != "running":
        retry += 1
        if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
            raise Exception(
                f"Container {container.name} did not reach 'running' state "
                f"after {CONTAINER_MAXIMUM_RETRY_COUNT} retries "
                f"(current status: {container.status})"
            )
        logger.info(
            f"Waiting for container {container.name} to be running "
            f"(status: {container.status}); retrying in {retry} second(s)..."
        )
        sleep(retry)
        container.reload()


def get_container_port_binding(container, port: str) -> int:
    port_binding = container.attrs["NetworkSettings"]["Ports"].get(port)
    if port_binding and len(port_binding) > 0:
        return int(port_binding[0]["HostPort"])
    else:
        raise Exception(f"Port {port} is not bound in the container.")
