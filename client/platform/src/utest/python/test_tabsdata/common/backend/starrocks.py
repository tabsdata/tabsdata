#
# Copyright 2026 Tabs Data Inc.
#

"""StarRocks fixtures and utilities."""

import logging
import os
from dataclasses import dataclass
from time import sleep

import pytest
from filelock import FileLock

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    CONTAINER_MAXIMUM_RETRY_COUNT,
    DB_HOST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX, DOCKER_REGULAR_CONTAINER_INFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_STARROCKS_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}starrocks"
)
STARROCKS_MYSQL_PORT = 9030
STARROCKS_MYSQL_FORWARDED_PORT = "9030/tcp"

VALID_STARROCKS_PREFIXES = ["STARROCKS0"]
STARROCKS_DB_USER = "root"
STARROCKS_DB_NAME = "test"
# Index of the 'Alive' column in SHOW BACKENDS output (0-based).
# See: https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS/
ALIVE_BACKEND_INDEX = 8


@dataclass
class StarRocksTestConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    credentials: UserPasswordCredentials


@pytest.fixture(scope="function")
def starrocks_client(starrocks_config):
    import pymysql

    conn = pymysql.connect(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        user=starrocks_config.user,
        password=starrocks_config.password,
        autocommit=True,
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")

    yield conn

    conn.close()


@pytest.fixture(scope="function")
def starrocks_config(tmp_path_factory, worker_id, request) -> StarRocksTestConfig:
    marker = request.node.get_closest_marker("starrocks")
    prefix = (
        marker.kwargs.get("env_prefix", "STARROCKS0") if marker else "STARROCKS0"
    )
    if prefix not in VALID_STARROCKS_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_STARROCKS_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_STARROCKS_PREFIXES' variable in "
            "the 'starrocks.py' file."
        )
    if os.environ.get(f"{prefix}__STARROCKS_ENVS") in TRUE_VALUES:
        host = os.environ.get(f"{prefix}__STARROCKS_HOST")
        if not host:
            raise Exception(
                f"The environment variable {prefix}__STARROCKS_HOST is not set. "
                "Unable to run tests using the 'starrocks_config' fixture."
            )
        port_str = os.environ.get(f"{prefix}__STARROCKS_PORT")
        if not port_str:
            raise Exception(
                f"The environment variable {prefix}__STARROCKS_PORT is not set. "
                "Unable to run tests using the 'starrocks_config' fixture."
            )
        user = os.environ.get(f"{prefix}__STARROCKS_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__STARROCKS_USER is not set. "
                "Unable to run tests using the 'starrocks_config' fixture."
            )
        password = os.environ.get(f"{prefix}__STARROCKS_PASSWORD", "")
        database = os.environ.get(f"{prefix}__STARROCKS_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__STARROCKS_DATABASE is not set. "
                "Unable to run tests using the 'starrocks_config' fixture."
            )
        return StarRocksTestConfig(
            host=host,
            port=int(port_str),
            user=user,
            password=password,
            database=database,
            credentials=UserPasswordCredentials(user, password),
        )
    else:
        name = os.environ.get(
            f"{prefix}__STARROCKS_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_STARROCKS_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            # not executing with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            port = create_docker_starrocks_database(name)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            fn = root_tmp_dir / "docker_starrocks_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                port = create_docker_starrocks_database(name)
        return StarRocksTestConfig(
            host=DB_HOST,
            port=int(port),
            user=STARROCKS_DB_USER,
            password="",
            database=STARROCKS_DB_NAME,
            credentials=UserPasswordCredentials(STARROCKS_DB_USER, ""),
        )


def _wait_for_database(port: int) -> None:
    """Poll until the test database is connectable via pymysql."""
    import pymysql

    retry = 0
    while True:
        try:
            verify_conn = pymysql.connect(
                host=DB_HOST,
                port=port,
                user=STARROCKS_DB_USER,
                password="",
                database=STARROCKS_DB_NAME,
                autocommit=True,
            )
            with verify_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            verify_conn.close()
            logger.info(f"Database '{STARROCKS_DB_NAME}' is ready")
            return
        except Exception:
            retry += 1
            if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
                raise Exception(
                    f"Database '{STARROCKS_DB_NAME}' not accessible after creation"
                )
            logger.warning(
                f"Database '{STARROCKS_DB_NAME}' not ready yet, "
                f"retrying in {retry} second(s)"
            )
            sleep(retry)


def create_docker_starrocks_database(
    name: str = DEFAULT_PYTEST_STARROCKS_DOCKER_CONTAINER_NAME,
) -> int:
    import docker  # Lazy import

    logger.info("Starting StarRocks container")
    client = docker.from_env()
    if containers := client.containers.list(filters={"name": name}):
        logger.info("StarRocks container already exists")
        container = containers[0]
        port = get_container_port_binding(container, STARROCKS_MYSQL_FORWARDED_PORT)
        # Ensure the test database exists even if container was already running
        import pymysql

        conn = pymysql.connect(
            host=DB_HOST,
            port=port,
            user=STARROCKS_DB_USER,
            password="",
            autocommit=True,
        )
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{STARROCKS_DB_NAME}`")
        conn.close()
        _wait_for_database(port)
        return port

    container = client.containers.run(
        "starrocks/allin1-ubuntu:latest",
        name=name,
        ports={STARROCKS_MYSQL_FORWARDED_PORT: None},
        detach=True,
    )
    container.reload()
    port = get_container_port_binding(container, STARROCKS_MYSQL_FORWARDED_PORT)

    # Wait for StarRocks FE to be ready (MySQL protocol)
    import pymysql  # Lazy import

    retry = 0
    conn = None
    while True:
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                port=port,
                user=STARROCKS_DB_USER,
                password="",
                autocommit=True,
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            break
        except Exception as err:
            retry += 1
            if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
                logger.error(f"Error connecting to StarRocks: {err}")
                raise err
            else:
                logger.warning(
                    f"Error connecting to StarRocks, retrying in {retry} second(s)"
                )
                sleep(retry)

    # Wait for at least one BE (Backend) to be alive — required for bulk loads
    retry = 0
    while True:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW BACKENDS")
                rows = cursor.fetchall()
                alive_backends = [
                    row for row in rows
                    if row[ALIVE_BACKEND_INDEX] is True
                    or row[ALIVE_BACKEND_INDEX] == "true"
                ]
                if alive_backends:
                    logger.info(
                        f"StarRocks has {len(alive_backends)} alive backend(s)"
                    )
                    break
        except Exception:
            pass
        retry += 1
        if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
            raise Exception(
                "StarRocks backend did not become alive within the timeout"
            )
        logger.warning(
            f"Waiting for StarRocks backend to be alive, retrying in {retry} second(s)"
        )
        sleep(retry)

    # Create test database and wait until it is connectable
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{STARROCKS_DB_NAME}`")
    conn.close()

    _wait_for_database(port)

    logger.info("StarRocks container created successfully")
    return port
