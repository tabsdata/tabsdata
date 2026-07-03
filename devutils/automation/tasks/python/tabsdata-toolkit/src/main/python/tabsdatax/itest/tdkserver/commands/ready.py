#
# Copyright 2026 Tabs Data Inc.
#

import json
import socket
import sys
import time
from pathlib import Path

import rich_click as click

TABSDATA_HOME_FOLDER = ".tabsdata"
INSTANCES_FOLDER = "instances"
SETTINGS_FILE = ".settings.json"
LOCALHOST = "127.0.0.1"
PORT_PROBE_TIMEOUT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_POLL_SECONDS = 5.0


def port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(PORT_PROBE_TIMEOUT_SECONDS)
        return probe.connect_ex((LOCALHOST, port)) == 0


def pending_ports(ports: list[dict]) -> list[str]:
    pending = []
    for entry in ports:
        port = int(entry["port"])
        if not port_listening(port):
            pending.append(f"{entry.get('caption', entry.get('role'))} ({port})")
    return pending


@click.command(name="ready")
@click.argument("instance")
@click.option("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
@click.option("--poll", type=float, default=DEFAULT_POLL_SECONDS)
def ready(instance: str, timeout: float, poll: float):
    """Wait until every declared port of an instance is listening.

    Polls until all ports accept connections or the timeout elapses. Exits 0
    when all are listening, 1 on timeout or missing settings.
    """
    settings_file = (
        Path.home() / TABSDATA_HOME_FOLDER / INSTANCES_FOLDER / instance / SETTINGS_FILE
    )
    if not settings_file.exists():
        click.echo(f"Settings file '{settings_file}' does not exist.", err=True)
        sys.exit(1)

    ports = json.loads(settings_file.read_text(encoding="utf-8")).get("ports", [])

    deadline = time.monotonic() + timeout
    pending = pending_ports(ports)
    while pending and time.monotonic() < deadline:
        time.sleep(poll)
        pending = pending_ports(ports)

    if pending:
        click.echo(
            f"Instance '{instance}' not ready after {timeout:.0f}s; "
            f"ports not listening: {', '.join(pending)}.",
            err=True,
        )
        sys.exit(1)
