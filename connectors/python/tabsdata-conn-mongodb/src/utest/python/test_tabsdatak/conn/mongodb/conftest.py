#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdatak.conn.mongodb.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from filelock import FileLock
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from test_tabsdata.common.constants import (
    CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS,
    CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS,
    DOCKER_REGULAR_CONTAINER_INFIX,
    DOCKER_UTEST_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)

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
# Docker-backed MongoDB fixture
#
# Mirrors the legacy ``tabsdata-mongodb`` conftest's
# ``create_docker_mongodb_database`` flow: bring up a single ``mongo``
# container with basic root credentials, expose 27017 on a random host
# port, and yield a config object the integration tests can hand
# straight to :class:`MongoDBConn`. Skipped (not failed) if the docker
# daemon is unreachable so unit-only runs without docker stay green.
# ---------------------------------------------------------------------------


_MONGO_IMAGE = "mongo:6.0.21"
_MONGO_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "mongodb"
)
_MONGO_PORT_INSIDE = "27017/tcp"
_MONGO_USER = "mongo_user"
_MONGO_PASSWORD = "mongo_password"
_MONGO_DATABASE = "td_integ"


@dataclass(frozen=True)
class MongoDBContainerConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def uri(self) -> str:
        """Connection URI WITHOUT credentials.

        :class:`MongoDBConn.credentials` carries the user/password; the
        plugin splices them into the URI at write time via
        :func:`_plugin._uri_with_credentials`. Embedding the credentials
        here would double them on the wire.
        """
        return (
            f"mongodb://{self.host}:{self.port}"
            "/?serverSelectionTimeoutMS=15000"
            "&connectTimeoutMS=15000"
            "&socketTimeoutMS=15000"
        )

    @property
    def uri_with_credentials(self) -> str:
        from urllib.parse import quote_plus

        return (
            f"mongodb://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}"
            "/?serverSelectionTimeoutMS=15000"
            "&connectTimeoutMS=15000"
            "&socketTimeoutMS=15000"
        )


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


def _container_port(container) -> int | None:
    ports = (container.attrs.get("NetworkSettings") or {}).get("Ports") or {}
    bindings = ports.get(_MONGO_PORT_INSIDE) or []
    if not bindings:
        return None
    try:
        return int(bindings[0]["HostPort"])
    except (KeyError, ValueError, IndexError):
        return None


def _wait_for_container_port(container) -> int | None:
    import time

    deadline = time.monotonic() + CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        container.reload()
        port = _container_port(container)
        if port is not None:
            return port
        time.sleep(CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS)
    return None


def _wait_for_mongo_ready(uri: str, timeout_s: int = 60) -> None:
    import time

    import pymongo

    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            client.close()
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(1.0)
    raise RuntimeError(
        f"MongoDB at {uri} not ready after {timeout_s}s: {last_err!r}"
    )


def _start_mongo_container() -> MongoDBContainerConfig:
    from docker.errors import APIError, ImageNotFound, NotFound

    client = _require_docker()

    try:
        existing = client.containers.get(_MONGO_CONTAINER_NAME)
    except NotFound:
        existing = None

    if existing is not None:
        if existing.status != "running":
            existing.start()
        host_port = _wait_for_container_port(existing)
        if host_port is None:
            existing.remove(force=True)
            existing = None
        else:
            cfg = MongoDBContainerConfig(
                host="127.0.0.1",
                port=host_port,
                user=_MONGO_USER,
                password=_MONGO_PASSWORD,
                database=_MONGO_DATABASE,
            )
            _wait_for_mongo_ready(cfg.uri_with_credentials)
            return cfg

    try:
        client.images.get(_MONGO_IMAGE)
    except ImageNotFound:
        try:
            logger.info("pulling docker image %s", _MONGO_IMAGE)
            client.images.pull(_MONGO_IMAGE)
        except APIError as exc:
            pytest.skip(f"docker image {_MONGO_IMAGE!r} pull failed: {exc}")

    try:
        container = client.containers.run(
            _MONGO_IMAGE,
            name=_MONGO_CONTAINER_NAME,
            environment=[
                f"MONGO_INITDB_ROOT_USERNAME={_MONGO_USER}",
                f"MONGO_INITDB_ROOT_PASSWORD={_MONGO_PASSWORD}",
            ],
            ports={_MONGO_PORT_INSIDE: None},
            detach=True,
        )
    except APIError as exc:
        pytest.skip(f"docker run {_MONGO_IMAGE!r} failed: {exc}")

    host_port = _wait_for_container_port(container)
    if host_port is None:
        container.remove(force=True)
        pytest.skip("docker container started but published no host port")

    cfg = MongoDBContainerConfig(
        host="127.0.0.1",
        port=host_port,
        user=_MONGO_USER,
        password=_MONGO_PASSWORD,
        database=_MONGO_DATABASE,
    )
    _wait_for_mongo_ready(cfg.uri_with_credentials)
    return cfg


@pytest.fixture(scope="session")
def mongodb_config(tmp_path_factory, worker_id) -> MongoDBContainerConfig:
    """Auto-started ``mongo`` container shared across the session.

    File-locked across xdist workers so only one process starts the
    container; the rest reuse it.
    """
    if worker_id == "master":
        return _start_mongo_container()
    root = tmp_path_factory.getbasetemp().parent
    with FileLock(str(root / "docker_mongodb.lock")):
        return _start_mongo_container()
