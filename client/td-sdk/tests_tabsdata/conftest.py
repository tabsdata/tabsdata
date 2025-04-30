#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import pathlib
import re
import sys
from time import sleep

import boto3
import cx_Oracle
import docker
import hvac
import mysql.connector
import numpy as np
import polars as pl
import psycopg2
import pytest
import yaml
from azure.storage.blob import BlobServiceClient
from filelock import FileLock

# The following non-import code must execute early to set up the environment correctly.
# Suppressing E402 to allow imports after this setup.
# flake8: noqa: E402

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("filelock").setLevel(logging.INFO)

TESTS_ROOT_FOLDER = os.path.dirname(__file__)

sys.path.insert(0, TESTS_ROOT_FOLDER)

from tests_tabsdata.bootest import TESTING_RESOURCES_PATH, check_assets, enrich_sys_path

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
check_assets()

import tabsdata.utils.tableframe._constants as td_constants
from tabsdata.api.apiserver import APIServer, APIServerError, obtain_connection
from tabsdata.api.tabsdata_server import TabsdataServer
from tabsdata.secret import HashiCorpSecret
from tabsdata.tabsdatafunction import TableInput, TableOutput
from tabsdata.tabsserver.function.sql_utils import MARIADB_COLLATION
from tabsdata.tabsserver.pyenv_creation import (
    DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER,
    delete_virtual_environment,
)
from tabsdata.utils.tableframe._generators import _id

ABSOLUTE_TEST_FOLDER_LOCATION = os.path.dirname(os.path.abspath(__file__))
ABSOLUTE_ROOT_FOLDER_LOCATION = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
logger.debug(f"ABSOLUTE_ROOT_FOLDER_LOCATION: {ABSOLUTE_ROOT_FOLDER_LOCATION}")

APISERVER_URL = "127.0.0.1:2467"
CORRECT_SOURCE = TableInput("input")
CORRECT_DESTINATION = TableOutput("output")

PYTEST_DEFAULT_ENVIRONMENT_PREFIX = "pytest_exclusive_prefix_"
DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME = "pytest_exclusive_mariadb_container"
DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME = "pytest_exclusive_mysql_container"
DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME = "pytest_exclusive_oracle_container"
DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME = "pytest_exclusive_postgres_container"
DEFAULT_LOGS_FILE = "fn.log"

DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME = "pytest_exclusive_hashicorp_container"
HASHICORP_PORT = 8200
HASHICORP_TESTING_SECRET_PATH = "tabsdata/testing/path"
HASHICORP_TESTING_SECRET_NAME = "testing_secret_name"
HASHICORP_TESTING_SECRET_VALUE = "testing_secret_value"
HASHICORP_TESTING_TOKEN = "testing_token"
HASHICORP_TESTING_URL = "http://127.0.0.1:8200"

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
LOCAL_PACKAGES_LIST = [
    ROOT_PROJECT_DIR,
    os.path.join(ROOT_PROJECT_DIR, "connectors", "python", "tabsdata_mongodb"),
    os.path.join(ROOT_PROJECT_DIR, "connectors", "python", "tabsdata_salesforce"),
]

TESTING_AWS_ACCESS_KEY_ID = "TRANSPORTER_AWS_ACCESS_KEY_ID"
TESTING_AWS_SECRET_ACCESS_KEY = "TRANSPORTER_AWS_SECRET_ACCESS_KEY"
TESTING_AZURE_ACCOUNT_NAME = "TRANSPORTER_AZURE_ACCOUNT_NAME"
TESTING_AZURE_ACCOUNT_KEY = "TRANSPORTER_AZURE_ACCOUNT_KEY"

DB_HOST = "127.0.0.1"
DB_NAME = "testing"
DB_PASSWORD = "p@ssw0rd#"
DB_USER = "@dmIn"
MARIADB_PORT = 3307
MYSQL_PORT = 3306
ORACLE_PORT = 1521
POSTGRES_PORT = 5432

FAKE_TRIGGERED_TIME = 1234567890123
FAKE_SCHEDULED_TIME = 1248067890123

# Expected output of the foo1 function when called with f1="o1" and f2="o2"
EXPECTED_FOO1_OUTPUT = ("o1", "o2")

FORMAT_TYPE_TO_CONFIG = {
    "csv": {
        "separator": ",",
        "quote_char": '"',
        "eol_char": "\n",
        "input_encoding": "Utf8",
        "input_null_values": None,
        "input_missing_is_null": True,
        "input_truncate_ragged_lines": False,
        "input_comment_prefix": None,
        "input_try_parse_dates": False,
        "input_decimal_comma": False,
        "input_has_header": True,
        "input_skip_rows": 0,
        "input_skip_rows_after_header": 0,
        "input_raise_if_empty": True,
        "input_ignore_errors": False,
        "output_include_header": True,
        "output_datetime_format": None,
        "output_date_format": None,
        "output_time_format": None,
        "output_float_scientific": None,
        "output_float_precision": None,
        "output_null_value": None,
        "output_quote_style": None,
        "output_maintain_order": True,
    },
    "json": {},
    "log": {},
    "parquet": {},
}

MAXIMUM_RETRY_COUNT = int(os.environ.get("PYTEST_MAXIMUM_RETRY_COUNT", "36"))
if not os.environ.get("TD_CLI_SHOW"):
    os.environ["TD_CLI_SHOW"] = "false"


def pytest_addoption(parser):
    parser.addoption(
        "--performance-size",
        help="Amount of records per table when running performance tests",
        default=25000,
        type=int,
    )


def pytest_generate_tests(metafunc):
    if "size" in metafunc.fixturenames:
        metafunc.parametrize("size", [metafunc.config.getoption("performance_size")])


def read_json_and_clean(path):
    """Reads a json file and removes the td.id column. Also sorts it to make
    comparison more stable"""
    df = pl.read_json(path)
    df = clean_polars_df(df)
    return df


def clean_polars_df(df) -> pl.DataFrame:
    """Removes the td column. Also sorts it to make comparison more stable"""
    # TODO: change to quote

    df = df.select(pl.exclude(f"^{re.escape(td_constants.TD_COLUMN_PREFIX)}.*$"))
    df = df.select(sorted(df.columns))
    df = df.sort(df.columns)
    return df


@pytest.fixture(scope="session")
def testing_mariadb(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_mariadb_database()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mariadb_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield create_docker_mariadb_database()


@pytest.fixture(scope="session")
def testing_hashicorp_vault(tmp_path_factory, worker_id):
    os.environ[HashiCorpSecret.VAULT_URL_ENV_VAR] = HASHICORP_TESTING_URL
    os.environ[HashiCorpSecret.VAULT_TOKEN_ENV_VAR] = HASHICORP_TESTING_TOKEN
    # Needed for test_input_s3_hashicorp_secret_vault_name to work
    os.environ[HashiCorpSecret.VAULT_URL_ENV_VAR.replace("HASHICORP", "H1")] = (
        HASHICORP_TESTING_URL
    )
    os.environ[HashiCorpSecret.VAULT_TOKEN_ENV_VAR.replace("HASHICORP", "H1")] = (
        HASHICORP_TESTING_TOKEN
    )
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_hashicorp_vault()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_hashicorp_vault_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield create_docker_hashicorp_vault()


@pytest.fixture(scope="session")
def testing_oracle(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_oracle_database()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_oracle_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield create_docker_oracle_database()


@pytest.fixture(scope="session")
def testing_mysql(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_mysql_database()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_mysql_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield create_docker_mysql_database()


def create_docker_postgres_database():
    logger.info("Starting Postgres container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME}
    ):
        logger.info("Postgres container already exists")
        return
    else:
        client.containers.run(
            "postgres:17.2",
            name=DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
            environment=[
                f"POSTGRES_DB={DB_NAME}",
                f"POSTGRES_USER={DB_USER}",
                f"POSTGRES_PASSWORD={DB_PASSWORD}",
            ],
            ports={"5432/tcp": POSTGRES_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            connection_params = {
                "dbname": DB_NAME,
                "user": DB_USER,
                "password": DB_PASSWORD,
                "host": DB_HOST,
                "port": str(POSTGRES_PORT),
            }
            try:
                mydb = psycopg2.connect(**connection_params)
                break
            except Exception as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to Postgres: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to Postgres, retrying in {retry} second(s)"
                    )
                    sleep(retry)
        mycursor = mydb.cursor()
        mycursor.execute(
            "CREATE TABLE INVOICE_HEADER (id int GENERATED ALWAYS AS IDENTITY PRIMARY"
            " KEY, name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_HEADER (name) VALUES (%s)"
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
            "CREATE TABLE INVOICE_ITEM (id int GENERATED ALWAYS AS IDENTITY PRIMARY "
            "KEY, name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_ITEM (name) VALUES (%s)"
        val = [
            ("Leonardo",),
            ("Donatello",),
            ("Michelangelo",),
            ("Raphael",),
            ("Splinter",),
        ]
        mycursor.executemany(sql, val)
        mycursor.execute(
            "CREATE TABLE output_postgres_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_postgres_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute("CREATE SCHEMA testing_schema")
        mydb.commit()
        logger.info("Postgres container created successfully")
        return


@pytest.fixture(scope="session")
def testing_postgres(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        yield create_docker_postgres_database()
    else:
        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent

        fn = root_tmp_dir / "docker_postgresql_creation"
        with FileLock(str(fn) + ".lock"):
            # only one worker will be able to create the database
            yield create_docker_postgres_database()


@pytest.fixture(scope="session")
def apiserver_connection() -> APIServer:
    return obtain_connection(APISERVER_URL, "admin", "tabsdata")


@pytest.fixture(scope="session")
def tabsserver_connection() -> TabsdataServer:
    return TabsdataServer(APISERVER_URL, "admin", "tabsdata")


@pytest.fixture
def s3_client():
    session = boto3.Session(
        aws_access_key_id=os.environ.get(TESTING_AWS_ACCESS_KEY_ID, "FAKE_ID"),
        aws_secret_access_key=os.environ.get(TESTING_AWS_SECRET_ACCESS_KEY, "FAKE_KEY"),
    )
    yield session.client("s3")


@pytest.fixture
def azure_client():
    account_name = os.environ.get(TESTING_AZURE_ACCOUNT_NAME, "FAKE_NAME")
    account_key = os.environ.get(TESTING_AZURE_ACCOUNT_KEY, "FAKE_KEY")
    service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    yield service


def create_docker_mysql_database():
    logger.info("Starting MySQL container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME}
    ):
        logger.info("MySQL container already exists")
        return
    else:
        client.containers.run(
            "mysql:9.1.0",
            name=DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
            environment=[
                "MYSQL_ROOT_PASSWORD=password",
                f"MYSQL_DATABASE={DB_NAME}",
                f"MYSQL_USER={DB_USER}",
                f"MYSQL_PASSWORD={DB_PASSWORD}",
            ],
            ports={"3306/tcp": MYSQL_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                mydb = mysql.connector.connect(
                    host=DB_HOST,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                )
                break
            except mysql.connector.Error as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to MySQL: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to MySQL, retrying in {retry} second(s)"
                    )
                    sleep(retry)
        mycursor = mydb.cursor()
        mycursor.execute(
            "CREATE TABLE INVOICE_HEADER (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_HEADER (name) VALUES (%s)"
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
            "CREATE TABLE INVOICE_ITEM (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_ITEM (name) VALUES (%s)"
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
        mydb.commit()
        logger.info("MySQL container created successfully")
        return


def create_docker_hashicorp_vault():
    logger.info("Starting HashiCorp vault container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME}
    ):
        logger.info("HashiCorp vault container already exists")
        return
    else:
        client.containers.run(
            "hashicorp/vault",
            name=DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
            environment=[
                f"VAULT_DEV_ROOT_TOKEN_ID={HASHICORP_TESTING_TOKEN}",
            ],
            ports={"8200/tcp": HASHICORP_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                hashicorp_client = hvac.Client(
                    url=HASHICORP_TESTING_URL,
                    token=HASHICORP_TESTING_TOKEN,
                    verify=True,
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path=HASHICORP_TESTING_SECRET_PATH,
                    secret={
                        HASHICORP_TESTING_SECRET_NAME: HASHICORP_TESTING_SECRET_VALUE
                    },
                )
                # Needed for test_input_s3_hashicorp_secret to work
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="aws/s3creds",
                    secret={
                        "access_key_id": os.environ.get(
                            TESTING_AWS_ACCESS_KEY_ID, "FAKE_ID"
                        )
                    },
                )
                # Needed for the config_resolver tests to work
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/tabsdata/dev",
                    secret={"jwt_secret": "jwt_secret_value"},
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/tabsdata/dev/s3a",
                    secret={
                        "bucket": "bucket_value",
                        "region": "region_value",
                        "access_key": "access_key_value",
                        "secret_key": "secret_key_value",
                    },
                )
                hashicorp_client.secrets.kv.v2.create_or_update_secret(
                    path="/td/dev/aza",
                    secret={
                        "azure_account_name": "azure_account_name_value",
                        "azure_account_key": "azure_account_key_value",
                    },
                )
                return
            except Exception as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to HashiCorp vault: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to HashiCorp vault, retrying in {retry} "
                        "second(s)"
                    )
                    sleep(retry)


def create_docker_mariadb_database():
    logger.info("Starting MariaDB container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME}
    ):
        logger.info("MariaDB container already exists")
        return
    else:
        client.containers.run(
            "mariadb:11.4.4",
            name=DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
            environment=[
                "MARIADB_ROOT_PASSWORD=password",
                f"MARIADB_DATABASE={DB_NAME}",
                f"MARIADB_USER={DB_USER}",
                f"MARIADB_PASSWORD={DB_PASSWORD}",
            ],
            ports={"3306/tcp": MARIADB_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                mydb = mysql.connector.connect(
                    host=DB_HOST,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    port=MARIADB_PORT,
                    collation=MARIADB_COLLATION,
                )
                break
            except mysql.connector.Error as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to MariaDB: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to MariaDB, retrying in {retry} second(s)"
                    )
                    sleep(retry)
        mycursor = mydb.cursor()
        mycursor.execute(
            "CREATE TABLE INVOICE_HEADER (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_HEADER (name) VALUES (%s)"
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
            "CREATE TABLE INVOICE_ITEM (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(255))"
        )
        sql = "INSERT INTO INVOICE_ITEM (name) VALUES (%s)"
        val = [
            ("Leonardo",),
            ("Donatello",),
            ("Michelangelo",),
            ("Raphael",),
            ("Splinter",),
        ]
        mycursor.executemany(sql, val)
        mycursor.execute(
            "CREATE TABLE output_mariadb_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_mariadb_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mydb.commit()
        logger.info("MariaDB container created successfully")
        return


def create_docker_oracle_database():
    logger.info("Starting Oracle container")
    client = docker.from_env()
    if client.containers.list(
        filters={"name": DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME}
    ):
        logger.info("Oracle container already exists")
        return
    else:
        client.containers.run(
            "container-registry.oracle.com/database/free:23.6.0.0-lite",
            name=DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
            environment=[
                f"ORACLE_PWD={DB_PASSWORD}",
            ],
            ports={"1521/tcp": ORACLE_PORT},
            detach=True,
        )
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                dsn_tns = cx_Oracle.makedsn(DB_HOST, ORACLE_PORT, service_name="FREE")
                mydb = cx_Oracle.connect(
                    user="system", password=DB_PASSWORD, dsn=dsn_tns
                )
                break
            except (Exception, cx_Oracle.DatabaseError) as err:
                retry += 1
                # Waiting for up to 10' & 30'', as young Gauss already knew.
                if retry == MAXIMUM_RETRY_COUNT:
                    logger.error(f"Error connecting to Oracle: {err}")
                    raise err
                else:
                    logger.warning(
                        f"Error connecting to Oracle, retrying in {retry} second(s)"
                    )
                    sleep(retry)
        mycursor = mydb.cursor()
        mycursor.execute(
            "CREATE TABLE INVOICE_HEADER (id INT GENERATED BY DEFAULT AS IDENTITY, name"
            " VARCHAR2(255))"
        )
        names = ["Arvind", "Tucu", "Dimas", "Joaquin", "Jennifer", "Aleix"]
        for name in names:
            sql = f"INSERT INTO INVOICE_HEADER (name) VALUES ('{name}')"
            mycursor.execute(sql)
        mycursor.execute(
            "CREATE TABLE INVOICE_ITEM (id INT GENERATED BY DEFAULT AS IDENTITY, name"
            " VARCHAR2(255))"
        )
        names = ["Leonardo", "Donatello", "Michelangelo", "Raphael", "Splinter"]
        for name in names:
            sql = f"INSERT INTO INVOICE_ITEM (name) VALUES ('{name}')"
            mycursor.execute(sql)
        mycursor.execute(
            'CREATE TABLE output_oracle_list ("Duration" INT, '
            '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE output_oracle_driver_provided ("Duration" INT, '
            '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE second_output_oracle_list ("Duration" INT, '
            '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE output_oracle_transaction ("Duration" INT, '
            '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE second_output_oracle_transaction ("Duration" INT, '
            '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mydb.commit()
        logger.info("Oracle container created successfully")
        return


def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()
    try:
        name_pattern = (
            f"({DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME}"
            f"|{DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME})"
        )
        remove_docker_containers(name_pattern)
    except Exception as e:
        logger.warning(
            "Error removing Docker containers. You can safely ignore it if running"
            f" tests on a Docker-less machine: {e}"
        )


def clean_python_virtual_environments():
    try:
        existing_virtual_environments = os.listdir(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER)
        logger.info(f"Existing virtual environments: {existing_virtual_environments}")
        for environment in existing_virtual_environments:
            if PYTEST_DEFAULT_ENVIRONMENT_PREFIX in environment:
                with open(
                    os.path.join(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER, environment), "r"
                ) as f:
                    real_name = f.read()
                logger.info(
                    f"Cleaning up environment {environment} with real name {real_name}"
                )
                delete_virtual_environment(environment, real_name)
    except FileNotFoundError:
        logger.info("No virtual environments to clean up.")


def remove_docker_containers(name_pattern):
    client = docker.from_env()
    for container in client.containers.list(filters={"name": name_pattern}):
        try:
            logger.info(f"Removing container {container}")
            container.remove(v=True, force=True)
        except Exception as e:
            logger.error(f"Failed to remove container {container}. Error: {e}")


class V1ExecutionContextFormat:
    """Simple class to enable us to generate yaml files for testing that emulate the
    ones generated by the rust backend"""

    def __init__(self, content):
        self.content = content


class MockTable:
    def __init__(self, content):
        self.content = content


class MockTableVersions:
    def __init__(self, content):
        self.content = content


def v1_execution_context_format_representer(
    dumper: yaml.SafeDumper, v1_execution_context_format: V1ExecutionContextFormat
) -> yaml.nodes.MappingNode:
    """Represent a V1_yaml instance as a YAML mapping node."""
    dumper.add_representer(MockTable, v1_mock_table_representer)
    dumper.add_representer(MockTableVersions, v1_mock_table_versions_representer)
    return dumper.represent_mapping("!V1", v1_execution_context_format.content)


def v1_mock_table_representer(
    dumper: yaml.SafeDumper, mock_table: MockTable
) -> yaml.nodes.MappingNode:
    """Represent a MockTable instance as a YAML mapping node."""
    return dumper.represent_mapping("!Table", mock_table.content)


def v1_mock_table_versions_representer(
    dumper: yaml.SafeDumper, mock_table_versions: MockTableVersions
) -> yaml.nodes.MappingNode:
    """Represent a MockTable instance as a YAML mapping node."""
    return dumper.represent_sequence("!TableVersions", mock_table_versions.content)


def get_dumper():
    """Add representers to a YAML seriailizer."""
    safe_dumper = yaml.SafeDumper
    safe_dumper.add_representer(
        V1ExecutionContextFormat, v1_execution_context_format_representer
    )
    return safe_dumper


def write_v1_yaml_file(
    filename: str,
    bundle_archive_location: str,
    mock_dependency_location: list[str] | None = None,
    mock_table_location: list[str] | None = None,
    input_initial_values_path: str | None = None,
    output_initial_values_path: str | None = None,
):
    content = {
        "info": {
            "function_bundle": {
                "uri": pathlib.Path(bundle_archive_location).as_uri(),
                "env_prefix": None,
            },
            "dataset_data_version": "fake_dataset_version",
            "triggered_on": FAKE_TRIGGERED_TIME,
            "execution_plan_triggered_on": FAKE_SCHEDULED_TIME,
        },
        "input": [],
    }
    if mock_dependency_location:
        for mocked_table in mock_dependency_location:
            if isinstance(mocked_table, str):
                uri = (
                    pathlib.Path(mocked_table).as_uri()
                    if mocked_table != "null"
                    else None
                )
                table_content = {
                    "name": "mocked_table",
                    "location": {
                        "uri": uri,
                        "env_prefix": None,
                    },
                }

                content["input"].append(MockTable(table_content))
            elif isinstance(mocked_table, list):
                table_versions = []
                for table in mocked_table:
                    uri = pathlib.Path(table).as_uri() if table != "null" else None
                    table_content = {
                        "name": "mocked_table",
                        "location": {
                            "uri": uri,
                            "env_prefix": None,
                        },
                    }
                    table_versions.append(table_content)
                content["input"].append(MockTableVersions(table_versions))
            else:
                raise ValueError(
                    f"Unexpected type {type(mocked_table)} for mock_dependency_location"
                )
    location = None
    if input_initial_values_path:
        location = {
            "uri": pathlib.Path(input_initial_values_path).as_uri(),
            "env_prefix": None,
        }
    table_content = {"name": "td-initial-values", "location": location}
    content["system_input"] = [MockTable(table_content)]

    content["output"] = []
    if mock_table_location:
        content["output"] = add_mock_table_location(mock_table_location)
    location = None
    if output_initial_values_path:
        location = {
            "uri": pathlib.Path(output_initial_values_path).as_uri(),
            "env_prefix": None,
        }
    table_content = {
        "name": "td-initial-values",
        "location": location,
    }
    content["system_output"] = [MockTable(table_content)]
    v1_execution_context = V1ExecutionContextFormat(content)
    with open(filename, "w") as stream:
        stream.write(yaml.dump(v1_execution_context, Dumper=get_dumper()))


def add_mock_table_location(mock_table_location):
    output = []
    for mocked_table in mock_table_location:
        if isinstance(mocked_table, str):
            uri = (
                pathlib.Path(mocked_table).as_uri() if mocked_table != "null" else None
            )
            table_content = {
                "name": "mocked_table",
                "location": {
                    "uri": uri,
                    "env_prefix": None,
                },
            }

            output.append(MockTable(table_content))
        elif isinstance(mocked_table, list):
            table_versions = []
            for table in mocked_table:
                uri = pathlib.Path(table).as_uri() if table != "null" else None
                table_content = {
                    "name": "mocked_table",
                    "location": {
                        "uri": uri,
                        "env_prefix": None,
                    },
                }
                table_versions.append(table_content)
            output.append(MockTableVersions(table_versions))
        else:
            raise ValueError(
                f"Unexpected type {type(mocked_table)} for mock_table_location"
            )
    return output


@pytest.fixture(scope="session")
def testing_collection_with_table(worker_id, tabsserver_connection):
    random_id = _id()
    collection_name = f"testing_collection_with_table_{worker_id}_{random_id}"
    file_path = os.path.join(
        ABSOLUTE_TEST_FOLDER_LOCATION,
        "testing_resources",
        "test_input_file_csv_string_format",
        "example.py",
    )
    function_path = file_path + "::input_file_csv_string_format"
    tabsserver_connection.collection_create(collection_name, description="description")
    tabsserver_connection.function_create(
        collection_name, function_path, local_packages=LOCAL_PACKAGES_LIST
    )
    tabsserver_connection.function_trigger(
        collection_name, "input_file_csv_string_format"
    )
    retry = 0
    while True:
        try:
            tabsserver_connection.table_sample(collection_name, "output")
            break
        except APIServerError as e:
            logger.debug(f"Error sampling table '{random_id}' - '{file_path}': {e}")
            logger.debug(f"Retrying '{random_id}' - '{file_path}' in {retry} seconds")
            retry += 1
            # Waiting for up to 10' & 30'', as young Gauss already knew.
            if retry == MAXIMUM_RETRY_COUNT:
                raise e
            else:
                sleep(retry)
    try:
        yield collection_name
    finally:
        # ToDo: Deleting collections is not yet supported
        #       In any case, as log as identifiers are unique, there seems to be no
        #       strong
        #       need to delete the collections after a test.
        pass
    #     try:
    #         logger.debug(f"Deleting collection {collection_name}")
    #         tabsserver_connection.collection_delete(collection_name)
    #     except APIServerError as e:
    #         logger.error(f"Failed to delete collection: {e}")


def get_lf(size: int, chunk_size=1000):
    final_lf = None
    for start in range(0, size, chunk_size):
        end = min(start + chunk_size, size)
        id_column = range(start, end)
        lf = pl.LazyFrame(
            {
                "id": id_column,
                "name": [f"name_{i}" for i in id_column],
                "value": np.random.rand(end - start),
            }
        )
        final_lf = pl.concat([final_lf, lf]) if final_lf is not None else lf
    return final_lf
