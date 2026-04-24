#
# Copyright 2026 Tabs Data Inc.
#

"""ClickHouse fixtures and utilities."""

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
    DB_PASSWORD,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX, DOCKER_REGULAR_CONTAINER_INFIX,
)
from test_tabsdata.common.container import (
    get_container_port_binding,
    wait_for_container_running,
)

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_CLICKHOUSE_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}clickhouse"
)
CLICKHOUSE_PORT = 8123
CLICKHOUSE_FORWARDED_PORT = "8123/tcp"

VALID_CLICKHOUSE_PREFIXES = ["CLICKHOUSE0"]
CLICKHOUSE_DB_USER = "default"
CLICKHOUSE_DB_NAME = "default"


@dataclass
class ClickHouseTestConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    http_url: str
    credentials: UserPasswordCredentials


@pytest.fixture(scope="function")
def clickhouse_client(clickhouse_config):
    # noinspection PyPackageRequirements

    import clickhouse_connect

    ch_client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
    )
    ch_client.query("SELECT 1")

    yield ch_client


@pytest.fixture(scope="session")
def clickhouse_config(tmp_path_factory, worker_id, request) -> ClickHouseTestConfig:
    marker = request.node.get_closest_marker("clickhouse")
    prefix = marker.kwargs.get("env_prefix", "CLICKHOUSE0") if marker else "CLICKHOUSE0"
    if prefix not in VALID_CLICKHOUSE_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_CLICKHOUSE_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_CLICKHOUSE_PREFIXES' variable in "
            "the 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}__CLICKHOUSE_ENVS") in TRUE_VALUES:
        host = os.environ.get(f"{prefix}__CLICKHOUSE_HOST")
        if not host:
            raise Exception(
                f"The environment variable {prefix}__CLICKHOUSE_HOST is not set. "
                "Unable to run tests using the 'clickhouse_config' fixture."
            )
        port_str = os.environ.get(f"{prefix}__CLICKHOUSE_PORT")
        if not port_str:
            raise Exception(
                f"The environment variable {prefix}__CLICKHOUSE_PORT is not set. "
                "Unable to run tests using the 'clickhouse_config' fixture."
            )
        user = os.environ.get(f"{prefix}__CLICKHOUSE_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__CLICKHOUSE_USER is not set. "
                "Unable to run tests using the 'clickhouse_config' fixture."
            )
        password = os.environ.get(f"{prefix}__CLICKHOUSE_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__CLICKHOUSE_PASSWORD is not set. "
                "Unable to run tests using the 'clickhouse_config' fixture."
            )
        database = os.environ.get(f"{prefix}__CLICKHOUSE_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__CLICKHOUSE_DATABASE is not set. "
                "Unable to run tests using the 'clickhouse_config' fixture."
            )
        return ClickHouseTestConfig(
            host=host,
            port=int(port_str),
            user=user,
            password=password,
            database=database,
            http_url=f"http://{host}:{port_str}/{database}",
            credentials=UserPasswordCredentials(user, password),
        )
    else:
        name = os.environ.get(
            f"{prefix}__CLICKHOUSE_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_CLICKHOUSE_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            # not executing with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            port = create_docker_clickhouse_database(name)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            fn = root_tmp_dir / "docker_clickhouse_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                port = create_docker_clickhouse_database(name)
        return ClickHouseTestConfig(
            host=DB_HOST,
            port=int(port),
            user=CLICKHOUSE_DB_USER,
            password=DB_PASSWORD,
            database=CLICKHOUSE_DB_NAME,
            http_url=f"http://{DB_HOST}:{port}/{CLICKHOUSE_DB_NAME}",
            credentials=UserPasswordCredentials(CLICKHOUSE_DB_USER, DB_PASSWORD),
        )


def create_docker_clickhouse_database(
    name: str = DEFAULT_PYTEST_CLICKHOUSE_DOCKER_CONTAINER_NAME,
) -> int:
    # noinspection PyPackageRequirements
    import docker  # Lazy import
    import docker.types

    logger.info("Starting ClickHouse container")
    client = docker.from_env()
    if containers := client.containers.list(all=True, filters={"name": name}):
        logger.info("ClickHouse container already exists")
        container = containers[0]
        wait_for_container_running(container)
        return get_container_port_binding(container, CLICKHOUSE_FORWARDED_PORT)

    try:
        container = client.containers.run(
            "clickhouse/clickhouse-server:latest",
            name=name,
            environment=[
                f"CLICKHOUSE_PASSWORD={DB_PASSWORD}",
                "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            ],
            ports={CLICKHOUSE_FORWARDED_PORT: None},
            ulimits=[
                docker.types.Ulimit(name="nofile", soft=262144, hard=262144),
            ],
            detach=True,
        )
    except Exception as e:
        logger.error(f"Failed to create ClickHouse container '{name}': {e}")
        raise
    container.reload()
    port = get_container_port_binding(container, CLICKHOUSE_FORWARDED_PORT)

    # Wait for ClickHouse to be ready
    import clickhouse_connect  # Lazy import

    retry = 0
    ch_client = None
    while True:
        try:
            ch_client = clickhouse_connect.get_client(
                host=DB_HOST,
                port=port,
                database=CLICKHOUSE_DB_NAME,
                username=CLICKHOUSE_DB_USER,
                password=DB_PASSWORD,
            )
            ch_client.query("SELECT 1")
            break
        except Exception as err:
            retry += 1
            if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
                logger.error(f"Error connecting to ClickHouse: {err}")
                raise err
            else:
                logger.warning(
                    f"Error connecting to ClickHouse, retrying in {retry} second(s)"
                )
                sleep(retry)

    # Create test tables
    ch_client.command(
        "CREATE TABLE INVOICE_HEADER "
        "(id UInt32, name String) "
        "ENGINE = MergeTree() ORDER BY id"
    )
    ch_client.command(
        "INSERT INTO INVOICE_HEADER (id, name) VALUES "
        "(1, 'Arvind'), (2, 'Tucu'), (3, 'Dimas'), "
        "(4, 'Joaquin'), (5, 'Jennifer'), (6, 'Aleix')"
    )
    ch_client.command(
        "CREATE TABLE INVOICE_ITEM "
        "(id UInt32, name String) "
        "ENGINE = MergeTree() ORDER BY id"
    )
    ch_client.command(
        "INSERT INTO INVOICE_ITEM (id, name) VALUES "
        "(1, 'Leonardo'), (2, 'Donatello'), (3, 'Michelangelo'), "
        "(4, 'Raphael'), (5, 'Splinter')"
    )
    ch_client.command(
        "CREATE TABLE output_sql_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch_client.command(
        "CREATE TABLE output_new_sql_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch_client.command(
        "CREATE TABLE output_new_sql_wo_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch_client.command(
        "CREATE TABLE second_output_sql_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch_client.command(
        "CREATE TABLE second_output_new_sql_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    ch_client.command(
        "CREATE TABLE second_output_new_sql_wo_transaction "
        "(Duration Int32, Pulse Int32, Maxpulse Int32, Calories Float64) "
        "ENGINE = MergeTree() ORDER BY tuple()"
    )
    logger.info("ClickHouse container created successfully")
    return port
