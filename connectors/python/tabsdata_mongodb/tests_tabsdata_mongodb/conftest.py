#
# Copyright 2025 Tabs Data Inc.
#
from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_mongodb.bootest import TESTING_RESOURCES_PATH

from tabsdata._utils.logging import setup_tests_logging

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import logging
import os
import subprocess
from time import sleep
from urllib.parse import quote_plus

import docker
import pymongo
import pytest
from filelock import FileLock
from tests_tabsdata.conftest import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    MAXIMUM_RETRY_COUNT,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    remove_docker_containers,
)

logger = logging.getLogger(__name__)


def pytest_configure():
    setup_tests_logging()


DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME = "pytest_exclusive_mongodb_container"
MONGODB_PORT = 27018
MONGODB_URI_WITHOUT_CREDENTIALS = f"mongodb://{DB_HOST}:{MONGODB_PORT}"
MONGODB_URI_WITH_CREDENTIALS = (
    "mongodb://"
    f"{quote_plus(DB_USER)}:"
    f"{quote_plus(DB_PASSWORD)}@{DB_HOST}:{MONGODB_PORT}"
)

DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME = "tests_tabsdata_mongodb"
DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME = (
    "pytest_exclusive_mongodb_container_with_replica_set"
)
MONGODB_WITH_REPLICA_SET_URI = "mongodb://127.0.0.1:27017/?replicaSet=rs0"


def create_docker_mongodb_database():
    remove_docker_containers(DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME)
    logger.info("Starting MongoDB container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_MONGODB_DOCKER_CONTAINER_NAME}
    ):
        logger.info("MongoDB container already exists")
        return
    else:
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
        # Wait for the database to be ready
        retry = 0
        while True:
            uri = MONGODB_URI_WITH_CREDENTIALS
            try:
                client = pymongo.MongoClient(uri)
                client.admin.command("ping")
                break
            except Exception as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to MongoDB: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to MongoDB, retrying in {retry} second(s)"
                    )
                    sleep(retry)
        logger.info("MongoDB container created successfully")
        return


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
            yield create_docker_mongodb_database()


def create_docker_mongodb_database_with_replica_set():
    remove_docker_containers(
        f"({DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME}"
        f"|{DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME})"
    )
    logger.info("Starting MongoDB container with replica set")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_MONGODB_WITH_REPLICA_SET_DOCKER_CONTAINER_NAME}
    ):
        logger.info("MongoDB container with replica set already exists")
        return
    else:
        compose_file = os.path.join(
            DEFAULT_PYTEST_MONGODB_DOCKER_COMPOSE_CONTAINER_NAME, "compose.yaml"
        )
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                compose_file,
                "up",
                "-d",
            ],
            check=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            uri = MONGODB_WITH_REPLICA_SET_URI
            try:
                client = pymongo.MongoClient(uri)
                client.admin.command("ping")
                break
            except Exception as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to MongoDB with replica set: {err}")
                    raise err
                else:
                    logger.warning(
                        "Error connecting to MongoDB with replica set, retrying in"
                        f" {retry} second(s)"
                    )
                    sleep(retry)
        logger.info("MongoDB container with replica set created successfully")
        return


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
            yield create_docker_mongodb_database_with_replica_set()


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()
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
