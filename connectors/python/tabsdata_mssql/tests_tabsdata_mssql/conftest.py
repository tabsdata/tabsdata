#
# Copyright 2025 Tabs Data Inc.
#

from tests_tabsdata_mssql.bootest import TESTING_RESOURCES_PATH

from tabsdata._utils.logging import setup_tests_logging
from tests_tabsdata.bootest import enrich_sys_path

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import logging
from time import sleep

import docker
import pytest
from filelock import FileLock

from tests_tabsdata.conftest import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    MAXIMUM_RETRY_COUNT,
    clean_polars_df,
    clean_python_virtual_environments,
    pytest_addoption,
    remove_docker_containers,
)

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME = (
    "pytest_exclusive_mssql_2019_container"
)
DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME = (
    "pytest_exclusive_mssql_2022_container"
)
import polars as pl

MSSQL_2019_PORT = 2544
MSSQL_2022_PORT = 1433
MSSQL_USER = "sa"

INVOICE_HEADER_DF = clean_polars_df(
    pl.DataFrame(
        {
            "id": range(1, 7),
            "name": ["Arvind", "Tucu", "Dimas", "Joaquin", "Jennifer", "Aleix"],
        }
    )
)
INVOICE_ITEM_DF = clean_polars_df(
    pl.DataFrame(
        {
            "id": range(1, 6),
            "name": ["Leonardo", "Donatello", "Michelangelo", "Raphael", "Splinter"],
        }
    )
)


def testing_mssql_2019(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return create_docker_mssql_database(
            DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME, MSSQL_2019_PORT, 2019
        )
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mssql_2019_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            return create_docker_mssql_database(
                DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME, MSSQL_2019_PORT, 2019
            )


def testing_mssql_2022(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return create_docker_mssql_database(
            DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME, MSSQL_2022_PORT, 2022
        )
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mssql_2022_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            return create_docker_mssql_database(
                DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME, MSSQL_2022_PORT, 2022
            )


def create_docker_mssql_database(name: str, port: int, version: int):
    logger.info("Starting Microsoft SQL Server container")
    logger.info(f"Using container name: {name}")
    logger.info(f"Using port: {port}")
    logger.info(f"Using version: {version}")
    client = docker.from_env()
    if client.containers.list(filters={"name": name}):
        logger.info("Microsoft SQL Server container already exists")
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_HOST},{port};"
            f"Database={DB_NAME};TrustServerCertificate=yes;"
        )
    else:
        import pyodbc

        client.containers.run(
            f"mcr.microsoft.com/mssql/server:{version}-latest",
            name=name,
            environment=[
                f"MSSQL_SA_PASSWORD={DB_PASSWORD}",
                "ACCEPT_EULA=Y",
            ],
            ports={"1433/tcp": port},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={DB_HOST},{port};UID={MSSQL_USER};"
            f"PWD={DB_PASSWORD};TrustServerCertificate=yes;"
        )
        while True:
            try:
                conn = pyodbc.connect(conn_str, autocommit=True)
                break
            except Exception as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to Microsoft SQL Server: {err}")
                    raise err
                else:
                    logger.warning(
                        "Error connecting to Microsoft SQL Server, retrying in"
                        f" {retry} second(s)"
                    )
                    sleep(retry)
        mycursor = conn.cursor()
        # Create the database
        mycursor.execute(f"CREATE DATABASE {DB_NAME}")
        mycursor.close()
        conn.close()
        # Create tables and insert data
        conn_str += f"Database={DB_NAME};"
        conn = pyodbc.connect(conn_str)
        mycursor = conn.cursor()
        mycursor.execute(
            "CREATE TABLE INVOICE_HEADER (id INT IDENTITY(1,1) PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_HEADER (name) VALUES (?)"
        val = [
            ("Arvind",),
            ("Tucu",),
            ("Dimas",),
            ("Joaquin",),
            ("Jennifer",),
            ("Aleix",),
        ]
        mycursor.executemany(sql, val)
        mycursor.execute(
            "CREATE TABLE INVOICE_ITEM (id INT IDENTITY(1,1) PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_ITEM (name) VALUES (?)"
        val = [
            ("Leonardo",),
            ("Donatello",),
            ("Michelangelo",),
            ("Raphael",),
            ("Splinter",),
        ]
        mycursor.executemany(sql, val)
        mycursor.execute(
            "CREATE TABLE output_sql_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_sql_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        conn.commit()
        logger.info("Microsoft SQL Server container created successfully")
        mycursor.close()
        conn.close()
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_HOST},{port};"
            f"Database={DB_NAME};TrustServerCertificate=yes;"
        )


def pytest_generate_tests(metafunc):
    if "mssql_connection" in metafunc.fixturenames:
        metafunc.parametrize(
            "mssql_connection", ["mssql_2019", "mssql_2022"], indirect=True
        )
    if "size" in metafunc.fixturenames:
        metafunc.parametrize("size", [metafunc.config.getoption("performance_size")])


@pytest.fixture
def mssql_connection(request, tmp_path_factory, worker_id):
    if request.param == "mssql_2019":
        return testing_mssql_2019(tmp_path_factory, worker_id)
    elif request.param == "mssql_2022":
        return testing_mssql_2022(tmp_path_factory, worker_id)
    else:
        raise ValueError("Invalid internal test config")


@pytest.fixture
def mssql_version(mssql_connection):
    """
    Fixture to return the version of the MSSQL connection.
    """
    if str(MSSQL_2019_PORT) in mssql_connection:
        return "2019"
    elif str(MSSQL_2022_PORT) in mssql_connection:
        return "2022"
    else:
        raise ValueError("Unknown MSSQL version in connection string")


def pytest_configure():
    setup_tests_logging()
    clean_everything()


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


def clean_everything():
    clean_python_virtual_environments()
    try:
        name_pattern = (
            f"({DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME})"
        )
        remove_docker_containers(name_pattern)
    except Exception as e:
        logger.warning(
            "Error removing Docker containers. You can safely ignore it if running"
            f" tests on a Docker-less machine: {e}"
        )
