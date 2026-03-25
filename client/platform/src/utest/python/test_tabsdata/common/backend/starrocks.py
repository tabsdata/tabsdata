#
# Copyright 2026 Tabs Data Inc.
#

"""StarRocks fixtures and utilities."""

import logging
import os
import re
from dataclasses import dataclass

import pytest
from filelock import FileLock

from tenacity import retry, stop_after_attempt, wait_incrementing

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
# Column indices in SHOW BACKENDS output (0-based).
# See: https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS/
ALIVE_BACKEND_INDEX = 8
AVAIL_CAPACITY_BACKEND_INDEX = 13
TOTAL_CAPACITY_BACKEND_INDEX = 14


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
    import mysql.connector

    conn = mysql.connector.connect(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        user=starrocks_config.user,
        password=starrocks_config.password,
        autocommit=True,
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        cursor.fetchall()

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
        port = -1
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


def _wait_for_alive_backends(conn) -> None:
    """Poll until all StarRocks BEs report as alive with available disk capacity."""

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def _poll():
        with conn.cursor() as cursor:
            cursor.execute("SHOW BACKENDS")
            rows = cursor.fetchall()
            if not rows:
                raise Exception("No StarRocks backends registered")
            not_alive = [
                row for row in rows
                if row[ALIVE_BACKEND_INDEX] is not True
                and row[ALIVE_BACKEND_INDEX] != "true"
            ]
            if not_alive:
                logger.warning(
                    f"Waiting for all StarRocks backends to be alive, "
                    f"{len(not_alive)} of {len(rows)} not ready"
                )
                raise Exception(
                    f"{len(not_alive)} backend(s) not alive yet"
                )
            no_capacity = []
            for row in rows:
                avail = str(row[AVAIL_CAPACITY_BACKEND_INDEX] or "").strip()
                total = str(row[TOTAL_CAPACITY_BACKEND_INDEX] or "").strip()
                # Extract leading numbers (e.g. "81.617 GB" -> 81.617).
                # During startup the BE may report placeholder values such
                # as AvailCapacity=1.000 B / TotalCapacity=0.000 B before
                # real disk stats are populated.
                avail_num = re.match(r"([0-9.]+)", avail)
                total_num = re.match(r"([0-9.]+)", total)
                if (
                    not total
                    or not total_num
                    or float(total_num.group(1)) == 0
                    or not avail
                    or not avail_num
                    or float(avail_num.group(1)) == 0
                ):
                    no_capacity.append(row)
            if no_capacity:
                logger.warning(
                    f"Waiting for StarRocks backends to report disk capacity; "
                    f"{len(no_capacity)} of {len(rows)} have no capacity yet "
                    f"(AvailCapacity: "
                    f"{[backend[AVAIL_CAPACITY_BACKEND_INDEX] for backend in no_capacity]}, "
                    f"TotalCapacity: "
                    f"{[backend[TOTAL_CAPACITY_BACKEND_INDEX] for backend in no_capacity]})"
                )
                raise Exception(
                    f"{len(no_capacity)} backend(s) have not reported "
                    f"disk capacity yet"
                )
            logger.info(
                f"All {len(rows)} StarRocks backend(s) alive with disk "
                f"capacity available"
            )

    _poll()


def _wait_for_fe(port: int):
    """Poll until the StarRocks FE is connectable via MySQL protocol."""
    import mysql.connector

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def _poll():
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=port,
            user=STARROCKS_DB_USER,
            password="",
            autocommit=True,
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        logger.info("StarRocks FE is ready")
        return conn

    return _poll()


def _wait_for_database(port: int) -> None:
    """Poll until the test database is connectable via MySQL protocol."""
    import mysql.connector

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def _poll():
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=port,
            user=STARROCKS_DB_USER,
            password="",
            database=STARROCKS_DB_NAME,
            autocommit=True,
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        conn.close()
        logger.info(f"Database '{STARROCKS_DB_NAME}' is ready")

    _poll()


def _prepare_docker_starrocks_database(port: int) -> None:
    """Wait for FE/BE readiness, configure replication, and create the test database."""
    conn = _wait_for_fe(port)
    _wait_for_alive_backends(conn)
    with conn.cursor() as cursor:
        cursor.execute(
            'admin set frontend config ("default_replication_num" = "1")'
        )
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{STARROCKS_DB_NAME}`")
    conn.close()
    _wait_for_database(port)


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
        _prepare_docker_starrocks_database(port)
        return port

    container = client.containers.run(
        "starrocks/allin1-ubuntu:latest",
        name=name,
        ports={STARROCKS_MYSQL_FORWARDED_PORT: None},
        detach=True,
    )
    container.reload()
    port = get_container_port_binding(container, STARROCKS_MYSQL_FORWARDED_PORT)
    _prepare_docker_starrocks_database(port)
    logger.info("StarRocks container created successfully")
    return port
