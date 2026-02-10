#
# Copyright 2026 Tabs Data Inc.
#

"""Docker containers utility functions."""


def get_container_port_binding(container, port: str) -> int:
    port_binding = container.attrs["NetworkSettings"]["Ports"].get(port)
    if port_binding and len(port_binding) > 0:
        return int(port_binding[0]["HostPort"])
    else:
        raise Exception(f"Port {port} is not bound in the container.")
