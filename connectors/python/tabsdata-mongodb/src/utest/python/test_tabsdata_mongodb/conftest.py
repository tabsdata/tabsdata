#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging

from test_tabsdata.common.constants import (
    DOCKER_COMPOSE_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import (
    pull_compose_images,
    pull_docker_image,
    wait_for_container_running,
)
from test_tabsdata.common.docker import remove_docker_containers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdata_mongodb.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

from urllib.parse import quote_plus

import pytest
from filelock import FileLock
from xdist.workermanage import WorkerController

from test_tabsdatak._testing import setup_tests_logging
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    DOCKER_REGULAR_CONTAINER_INFIX,
)
from test_tabsdata.common.constants import CONTAINER_MAXIMUM_RETRY_COUNT
from test_tabsdata.conftest import (
    clean_everything,
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


DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}mongodb"
)
MONGODB_PORT = 27018
MONGODB_TIMEOUT_MS = 300000  # 5 minutes
MONGODB_CONNECTION_OPTIONS = (
    "retryWrites=true&retryReads=true"
    f"&serverSelectionTimeoutMS={MONGODB_TIMEOUT_MS}"
    f"&connectTimeoutMS={MONGODB_TIMEOUT_MS}"
    f"&socketTimeoutMS={MONGODB_TIMEOUT_MS}"
)
MONGODB_URI_WITHOUT_CREDENTIALS = (
    f"mongodb://{DB_HOST}:{MONGODB_PORT}"
    f"/?{MONGODB_CONNECTION_OPTIONS}"
)
MONGODB_URI_WITH_CREDENTIALS = (
    "mongodb://"
    f"{quote_plus(DB_USER)}:"
    f"{quote_plus(DB_PASSWORD)}@{DB_HOST}:{MONGODB_PORT}"
    f"/?{MONGODB_CONNECTION_OPTIONS}"
)

DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_COMPOSE_CONTAINER_INFIX}mongodb"
)
DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}mongodb-rs"
)
MONGODB_WITH_REPLICA_SET_URI = (
    "mongodb://127.0.0.1:27017"
    f"/?replicaSet=rs0&{MONGODB_CONNECTION_OPTIONS}"
)


def wait_for_mongodb_ready(uri, label="MongoDB"):
    import pymongo  # Lazy import

    from tenacity import retry, stop_after_delay, wait_fixed

    max_delay = CONTAINER_MAXIMUM_RETRY_COUNT * (CONTAINER_MAXIMUM_RETRY_COUNT + 1) // 2

    @retry(
        stop=stop_after_delay(max_delay),
        wait=wait_fixed(2),
        reraise=True,
    )
    def try_connect():
        logger.info(f"Attempting to connect to {label}...")
        client = pymongo.MongoClient(uri)
        client.admin.command("ping")
        logger.info(f"Successfully connected to {label}")

    try:
        try_connect()
    except Exception as err:
        logger.error(f"Failed to connect to {label} after {max_delay}s: {err}")
        raise


def create_docker_mongodb_database():
    import docker  # Lazy import

    logger.info("Starting MongoDB container")
    client = docker.from_env()
    if containers := client.containers.list(
        all=True,
        filters={"name": f"^{DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME}$"}
    ):
        logger.info("MongoDB container already exists")
        wait_for_container_running(containers[0])
        wait_for_mongodb_ready(MONGODB_URI_WITH_CREDENTIALS)
        return
    remove_docker_containers(DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME)
    pull_docker_image("mongo:6.0.21")
    try:
        client.containers.run(
            "mongo:6.0.21",
            name=DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME,
            environment=[
                f"MONGO_INITDB_ROOT_USERNAME={DB_USER}",
                f"MONGO_INITDB_ROOT_PASSWORD={DB_PASSWORD}",
            ],
            ports={"27017/tcp": MONGODB_PORT},
            detach=True,
        )
    except Exception as e:
        logger.error(
            f"Failed to create MongoDB container "
            f"'{DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME}': {e}"
        )
        raise
    wait_for_mongodb_ready(MONGODB_URI_WITH_CREDENTIALS)
    logger.info("MongoDB container created successfully")


@pytest.fixture(scope="session")
def testing_mongodb(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_mongodb_database()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mongodb_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            create_docker_mongodb_database()
        yield


def create_docker_mongodb_database_with_replica_set():
    import os  # Lazy import
    import subprocess  # Lazy import

    import docker  # Lazy import

    logger.info("Starting MongoDB container with replica set")
    client = docker.from_env()
    if containers := client.containers.list(
        all=True,
        filters={"name": f"^{DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME}$"}
    ):
        logger.info("MongoDB container with replica set already exists")
        wait_for_container_running(containers[0])
        wait_for_mongodb_ready(
            MONGODB_WITH_REPLICA_SET_URI, label="MongoDB with replica set"
        )
        return
    remove_docker_containers(
        f"({DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME}"
        f"|{DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME})"
    )
    compose_file = os.path.join(os.path.dirname(__file__), "compose.yaml")
    pull_compose_images(compose_file)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            compose_file,
            "up",
            "-d",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(
            f"Failed to start MongoDB replica set compose stack "
            f"(exit code {result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr
        )
    wait_for_mongodb_ready(
        MONGODB_WITH_REPLICA_SET_URI, label="MongoDB with replica set"
    )
    logger.info("MongoDB container with replica set created successfully")


@pytest.fixture(scope="session")
def testing_mongodb_with_replica_set(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_mongodb_database_with_replica_set()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mongodb_with_replica_set_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            create_docker_mongodb_database_with_replica_set()
        yield


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()
    try:
        name_pattern = (
            f"({DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME})"
        )
        remove_docker_containers(name_pattern)
    except Exception as e:
        logger.warning(
            "Error removing Docker containers. You can safely ignore it if running"
            f" tests on a Docker-less machine: {e}"
        )
