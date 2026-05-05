#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import urllib
from dataclasses import dataclass

from test_tabsdata.common.constants import (
    DOCKER_REGULAR_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import pull_docker_image, wait_for_container_running

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.bootest import enrich_sys_path
from test_tabsdata_mssql.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

from time import sleep

import pytest
from filelock import FileLock
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    CONTAINER_MAXIMUM_RETRY_COUNT,
)
from test_tabsdata.common.docker import (
    remove_docker_containers,
)
from test_tabsdata.conftest import (
    clean_python_virtual_environments,
    pytest_addoption,
    setup_temp_folder,
    setup_temp_folder_node,
)

pytest_plugins = ["test_tabsdata.fntest"]


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}mssql-2019"
)
DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}mssql-2022"
)

MSSQL_2019_PORT = 2544
MSSQL_2022_PORT = 1433
MSSQL_FORWARDED_PORT = "1433/tcp"
MSSQL_USER = "sa"


# Lazy-loaded dataframes with lazy imports
def _create_invoice_dataframes():
    """Create dataframes with lazy polars import"""
    import polars as pl  # Lazy import

    from test_tabsdata.conftest import clean_polars_df

    invoice_header = clean_polars_df(
        pl.DataFrame(
            {
                "id": range(1, 7),
                "name": ["Arvind", "Tucu", "Dimas", "Joaquin", "Jennifer", "Aleix"],
            }
        )
    )
    invoice_item = clean_polars_df(
        pl.DataFrame(
            {
                "id": range(1, 6),
                "name": [
                    "Leonardo",
                    "Donatello",
                    "Michelangelo",
                    "Raphael",
                    "Splinter",
                ],
            }
        )
    )
    return invoice_header, invoice_item


INVOICE_HEADER_DF, INVOICE_ITEM_DF = _create_invoice_dataframes()

VALID_MSSQL_PREFIXES = ["MSSQL0"]


@dataclass
class MSSQLConfig:
    connection_string: str
    credentials: UserPasswordCredentials | None
    uri: str = ""
    version: int = 2019


def testing_mssql_2019(tmp_path_factory, worker_id, request) -> MSSQLConfig:
    marker = request.node.get_closest_marker("mssql")
    prefix = marker.kwargs.get("env_prefix", "MSSQL0") if marker else "MSSQL0"
    if prefix not in VALID_MSSQL_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_MSSQL_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_MSSQL_PREFIXES' variable in the"
            " 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}_MSSQL_2019_ENVS") in TRUE_VALUES:
        connection_string = os.environ.get(f"{prefix}_MSSQL_2019_CONNECTION_STRING")
        uri = os.environ.get(f"{prefix}_MSSQL_2019_URI")
        if not (connection_string or uri):
            raise Exception(
                f"Neither environment variables '{prefix}_MSSQL_2019_CONNECTION_STRING'"
                f"nor '{prefix}_MSSQL_2019_URI' are set. Unable to run tests using the"
                "'mssql_connection' fixture."
            )
        user = os.environ.get(f"{prefix}_MSSQL_2019_USER")
        if not user:
            raise Exception(
                f"The environment variable '{prefix}_MSSQL_2019_USER' is not set."
                " Unable to run tests using the 'mssql_connection' fixture."
            )
        password = os.environ.get(f"{prefix}_MSSQL_2019_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable '{prefix}_MSSQL_2019_PASSWORD' is not set."
                " Unable to run tests using the 'mssql_connection' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = MSSQLConfig(
            connection_string=connection_string,
            uri=uri,
            credentials=UserPasswordCredentials(
                user=escaped_user, password=escaped_password
            ),
        )
        return config
    else:
        port = None
        name = os.environ.get(
            f"{prefix}__MSSQL_2019_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_MSSQL_2019_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            result = create_docker_mssql_database(name, port, 2019)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / "docker_mssql_2019_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                result = create_docker_mssql_database(name, port, 2019)
        return MSSQLConfig(
            connection_string=result[0],
            uri=result[1],
            credentials=UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
        )


def testing_mssql_2022(tmp_path_factory, worker_id, request) -> MSSQLConfig:
    marker = request.node.get_closest_marker("mssql")
    prefix = marker.kwargs.get("env_prefix", "MSSQL0") if marker else "MSSQL0"
    if prefix not in VALID_MSSQL_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_MSSQL_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_MSSQL_PREFIXES' variable in the"
            " 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}_MSSQL_2022_ENVS") in TRUE_VALUES:
        connection_string = os.environ.get(f"{prefix}_MSSQL_2022_CONNECTION_STRING")
        uri = os.environ.get(f"{prefix}_MSSQL_2022_URI")
        if not (connection_string or uri):
            raise Exception(
                f"Neither environment variables '{prefix}_MSSQL_2022_CONNECTION_STRING'"
                f"nor '{prefix}_MSSQL_2022_URI' are set. Unable to run tests using the"
                "'mssql_connection' fixture."
            )
        user = os.environ.get(f"{prefix}_MSSQL_2022_USER")
        if not user:
            raise Exception(
                f"The environment variable '{prefix}_MSSQL_2022_USER' is not set."
                " Unable to run tests using the 'mssql_connection' fixture."
            )
        password = os.environ.get(f"{prefix}_MSSQL_2022_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable '{prefix}_MSSQL_2022_PASSWORD' is not set."
                " Unable to run tests using the 'mssql_connection' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = MSSQLConfig(
            connection_string=connection_string,
            uri=uri,
            credentials=UserPasswordCredentials(
                user=escaped_user, password=escaped_password
            ),
            version=2022,
        )
        return config
    else:
        port = None
        name = os.environ.get(
            f"{prefix}__MSSQL_2022_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_MSSQL_2022_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            result = create_docker_mssql_database(name, port, 2022)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / "docker_mssql_2022_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                result = create_docker_mssql_database(name, port, 2022)
        return MSSQLConfig(
            connection_string=result[0],
            uri=result[1],
            credentials=UserPasswordCredentials(MSSQL_USER, DB_PASSWORD),
            version=2022,
        )


def get_container_port_binding(container, port: str) -> int:
    port_binding = container.attrs["NetworkSettings"]["Ports"].get(port)
    if port_binding and len(port_binding) > 0:
        return int(port_binding[0]["HostPort"])
    else:
        raise Exception(f"Port {port} is not bound in the container.")


def create_docker_mssql_database(name: str, port: int, version: int) -> tuple[str, str]:
    import docker  # Lazy import

    logger.info("Starting Microsoft SQL Server container")
    logger.info(f"Using container name: {name}")
    logger.info(f"Using port: {port}")
    logger.info(f"Using version: {version}")
    client = docker.from_env()
    if containers := client.containers.list(all=True, filters={"name": f"^{name}$"}):
        logger.info("Microsoft SQL Server container already exists")
        container = containers[0]
        wait_for_container_running(container)
        port = get_container_port_binding(container, MSSQL_FORWARDED_PORT)
        return (
            (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_HOST},{port};"
                f"Database={DB_NAME};TrustServerCertificate=yes;"
            ),
            (
                f"mssql://{urllib.parse.quote(DB_HOST)}:{port}/{DB_NAME}?driver={urllib.parse.quote_plus('ODBC Driver 18 for SQL Server')}&TrustServerCertificate=yes"
            ),
        )
    else:
        import pyodbc

        pull_docker_image(f"mcr.microsoft.com/mssql/server:{version}-latest")
        try:
            container = client.containers.run(
                f"mcr.microsoft.com/mssql/server:{version}-latest",
                name=name,
                environment=[
                    f"MSSQL_SA_PASSWORD={DB_PASSWORD}",
                    "ACCEPT_EULA=Y",
                ],
                ports={MSSQL_FORWARDED_PORT: None},
                detach=True,
            )
        except Exception as e:
            logger.error(f"Failed to create MSSQL container '{name}': {e}")
            raise
        container.reload()
        port = get_container_port_binding(container, MSSQL_FORWARDED_PORT)
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
                if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
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
        mycursor.execute("""
            CREATE TABLE all_mssql_types (
                id INT PRIMARY KEY,
                c_bit BIT,
                c_tinyint TINYINT,
                c_smallint SMALLINT,
                c_int INT,
                c_bigint BIGINT,
                c_decimal DECIMAL(10,2),
                c_numeric NUMERIC(10,2),
                c_money MONEY,
                c_smallmoney SMALLMONEY,
                c_float FLOAT,
                c_real REAL,
                c_char CHAR(10),
                c_varchar VARCHAR(50),
                c_text TEXT,
                c_nchar NCHAR(10),
                c_nvarchar NVARCHAR(50),
                c_ntext NTEXT,
                c_binary BINARY(8),
                c_varbinary VARBINARY(16),
                c_image IMAGE,
                c_date DATE,
                c_datetime DATETIME,
                c_datetime2 DATETIME2,
                c_smalldatetime SMALLDATETIME,
                c_time TIME,
                c_datetimeoffset DATETIMEOFFSET,
                c_uniqueidentifier UNIQUEIDENTIFIER
            )
        """)
        sql = """
            INSERT INTO all_mssql_types (
                id, c_bit, c_tinyint, c_smallint, c_int, c_bigint, c_decimal, c_numeric,
                c_money, c_smallmoney, c_float, c_real, c_char, c_varchar, c_text,
                c_nchar, c_nvarchar, c_ntext, c_binary, c_varbinary, c_image, c_date,
                c_datetime, c_datetime2, c_smalldatetime, c_time, c_datetimeoffset, c_uniqueidentifier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        import uuid

        val1 = (
            1,
            1,
            255,
            32767,
            2147483647,
            9223372036854775807,
            12345.67,
            23456.78,
            100.50,
            200.25,
            3.14159,
            2.71828,
            "char10   ",
            "varchar50",
            "some text",
            "nchar10   ",
            "nvarchar50",
            "some ntext",
            b"\x01\x02\x03\x04\x05\x06\x07\x08",
            b"\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10",
            b"\x11\x12\x13\x14\x15\x16\x17\x18",
            "2024-01-01",
            "2024-01-01 12:34:56",
            "2024-01-01 12:34:56.789",
            "2024-01-01 12:34:00",
            "12:34:56",
            "2024-01-01T12:34:56+06:00",
            str(uuid.uuid4()),
        )
        val2 = (
            2,
            0,
            0,
            -32768,
            -2147483648,
            -9223372036854775808,
            -12345.67,
            -23456.78,
            -100.50,
            -200.25,
            -3.14159,
            -2.71828,
            "char2    ",
            "varchar2",
            "other text",
            "nchar2    ",
            "nvarchar2",
            "other ntext",
            b"\xaa\xbb\xcc\xdd\xee\xff\x00\x11",
            b"\x22\x33\x44\x55\x66\x77\x88\x99",
            b"\xab\xcd\xef\x01\x23\x45\x67\x89",
            "2025-02-02",
            "2025-02-02 23:45:01",
            "2025-02-02 23:45:01.123",
            "2025-02-02 23:45:00",
            "23:45:01",
            "2025-02-02T23:45:01-08:00",
            str(uuid.uuid4()),
        )
        mycursor.execute(sql, val1)
        mycursor.execute(sql, val2)
        conn.commit()
        logger.info("Microsoft SQL Server container created successfully")
        mycursor.close()
        conn.close()
        return (
            (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_HOST},{port};"
                f"Database={DB_NAME};TrustServerCertificate=yes;"
            ),
            (
                f"mssql://{urllib.parse.quote(DB_HOST)}:{port}/{DB_NAME}?driver={urllib.parse.quote_plus('ODBC Driver 18 for SQL Server')}&TrustServerCertificate=yes"
            ),
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
        return testing_mssql_2019(tmp_path_factory, worker_id, request)
    elif request.param == "mssql_2022":
        return testing_mssql_2022(tmp_path_factory, worker_id, request)
    else:
        raise ValueError("Invalid internal test config")


@pytest.fixture
def mssql_version(mssql_connection):
    """
    Fixture to return the version of the MSSQL connection.
    """
    return str(mssql_connection.version)


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
