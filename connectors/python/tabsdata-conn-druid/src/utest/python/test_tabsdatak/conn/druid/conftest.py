#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdatak.conn.druid.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from filelock import FileLock
from xdist.workermanage import WorkerController

from test_tabsdatak._testing import setup_tests_logging

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


# ---------------------------------------------------------------------------
# Docker-backed Druid cluster fixture
#
# Mirrors the legacy ``client/platform/.../druid.py`` flow: bring up the
# full Druid cluster (zookeeper + coordinator + broker + historical +
# middlemanager + router) declared in :file:`druid-compose.yaml` via
# ``docker compose -p <project> up -d``, then expose a config object the
# integration tests can hand straight to ``DruidDestConn``. Skipped (not
# failed) if docker isn't reachable so unit-only runs without docker
# stay green.
# ---------------------------------------------------------------------------


_DRUID_COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "druid-compose.yaml")
_DRUID_PROJECT_NAME = "tdpytest_c_conn_druid"

_DRUID_COORDINATOR_PORT_INSIDE = "8081/tcp"
_DRUID_BROKER_PORT_INSIDE = "8082/tcp"
_DRUID_ROUTER_PORT_INSIDE = "8888/tcp"

# Cold startup of the full stack on a fresh image pull regularly takes
# well over a minute; keep this loose so noisy CI machines don't flake.
_DRUID_READY_TIMEOUT_S = 300


@dataclass(frozen=True)
class DruidContainerConfig:
    host: str
    overlord_port: int
    broker_port: int
    router_port: int

    @property
    def overlord_url(self) -> str:
        return f"http://{self.host}:{self.overlord_port}"

    @property
    def broker_url(self) -> str:
        return f"http://{self.host}:{self.broker_port}"

    @property
    def router_url(self) -> str:
        return f"http://{self.host}:{self.router_port}"


def _require_docker():
    try:
        import docker
    except ImportError:
        pytest.skip("docker SDK not installed (`pip install docker`)")
    try:
        client = docker.from_env()
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"docker daemon unreachable: {exc}")
    return client


def _compose_service_container(client, project: str, service: str):
    """Return the running container for ``project/service``, or None."""
    containers = client.containers.list(
        all=True,
        filters={
            "label": [
                f"com.docker.compose.project={project}",
                f"com.docker.compose.service={service}",
            ]
        },
    )
    return containers[0] if containers else None


def _container_port(container, inside: str) -> int | None:
    container.reload()
    ports = (container.attrs.get("NetworkSettings") or {}).get("Ports") or {}
    bindings = ports.get(inside) or []
    if not bindings:
        return None
    try:
        return int(bindings[0]["HostPort"])
    except (KeyError, ValueError, IndexError):
        return None


def _wait_for_overlord_ready(host: str, port: int, timeout_s: int) -> None:
    import time

    import requests

    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    url = f"http://{host}:{port}/status"
    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2.0)
    raise RuntimeError(
        f"Druid overlord at {url} not ready after {timeout_s}s: {last_err!r}"
    )


def _start_druid_compose() -> DruidContainerConfig:
    client = _require_docker()

    # Reuse the stack if it's already up.
    coordinator = _compose_service_container(
        client, _DRUID_PROJECT_NAME, "coordinator"
    )
    if coordinator is None or coordinator.status != "running":
        logger.info(
            "starting docker compose stack '%s' from %s",
            _DRUID_PROJECT_NAME,
            _DRUID_COMPOSE_FILE,
        )
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                _DRUID_PROJECT_NAME,
                "-f",
                _DRUID_COMPOSE_FILE,
                "up",
                "-d",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(
                f"docker compose up failed for project {_DRUID_PROJECT_NAME!r}:\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        coordinator = _compose_service_container(
            client, _DRUID_PROJECT_NAME, "coordinator"
        )

    broker = _compose_service_container(client, _DRUID_PROJECT_NAME, "broker")
    router = _compose_service_container(client, _DRUID_PROJECT_NAME, "router")
    if coordinator is None or broker is None or router is None:
        pytest.skip(
            "expected coordinator/broker/router containers under compose project "
            f"{_DRUID_PROJECT_NAME!r} are missing"
        )

    overlord_port = _container_port(coordinator, _DRUID_COORDINATOR_PORT_INSIDE)
    broker_port = _container_port(broker, _DRUID_BROKER_PORT_INSIDE)
    router_port = _container_port(router, _DRUID_ROUTER_PORT_INSIDE)
    if not (overlord_port and broker_port and router_port):
        pytest.skip(
            "docker compose stack started but port bindings are missing "
            f"(overlord={overlord_port}, broker={broker_port}, "
            f"router={router_port})"
        )

    _wait_for_overlord_ready("127.0.0.1", overlord_port, _DRUID_READY_TIMEOUT_S)
    return DruidContainerConfig(
        host="127.0.0.1",
        overlord_port=overlord_port,
        broker_port=broker_port,
        router_port=router_port,
    )


@pytest.fixture(scope="session")
def druid_config(tmp_path_factory, worker_id) -> DruidContainerConfig:
    """Auto-started Druid cluster shared across the session.

    File-locked across xdist workers so only one process brings up the
    compose stack; the rest reuse it.
    """
    if worker_id == "master":
        return _start_druid_compose()
    root = tmp_path_factory.getbasetemp().parent
    with FileLock(str(root / "docker_druid.lock")):
        return _start_druid_compose()
