#
# Copyright 2026 Tabs Data Inc.
#

"""PostgreSQL fixtures and utilities."""

import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from time import sleep

import psycopg2
import pytest
from filelock import FileLock

from tabsdata._credentials import Credentials, UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    CONTAINER_MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}c_postgres"
)
POSTGRES_PORT = 5432
POSTGRES_FORWARDED_PORT = "5432/tcp"

VALID_POSTGRES_PREFIXES = ["POSTGRES0"]


@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    uri: str
    uri_with_credentials: str
    credentials: Credentials


@pytest.fixture(scope="function")
def postgres_config(tmp_path_factory, worker_id, request) -> PostgresConfig:
    marker = request.node.get_closest_marker("postgres")
    prefix = marker.kwargs.get("env_prefix", "POSTGRES0") if marker else "POSTGRES0"
    if prefix not in VALID_POSTGRES_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are:"
            f" {', '.join(VALID_POSTGRES_PREFIXES)}. If you want to extend the list of"
            " valid prefixes, please modify the 'VALID_POSTGRES_PREFIXES' variable in"
            " the 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}__POSTGRES_ENVS") in TRUE_VALUES:
        uri = os.environ.get(f"{prefix}__POSTGRES_URI")
        if not uri:
            raise Exception(
                f"The environment variable {prefix}__POSTGRES_URI is not set. Unable to"
                " run tests using the 'postgres_config' fixture."
            )
        pattern = r"^postgres://([^:/]+):(\d+)/(.*)$"
        match = re.match(pattern, uri)
        if not match:
            raise Exception(
                f"The POSTGRES URI '{uri}' is not valid. Unable to run tests using the "
                "'postgres_config' fixture."
            )
        host, port, _ = match.groups()
        user = os.environ.get(f"{prefix}__POSTGRES_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__POSTGRES_USER is not set. Unable"
                " to run tests using the 'postgres_config' fixture."
            )
        password = os.environ.get(f"{prefix}__POSTGRES_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__POSTGRES_PASSWORD is not set."
                " Unable to run tests using the 'postgres_config' fixture."
            )
        database = os.environ.get(f"{prefix}__POSTGRES_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__POSTGRES_DATABASE is not set."
                " Unable to run tests using the 'postgres_config' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = PostgresConfig(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            uri=uri,
            uri_with_credentials=(
                f"postgres://{escaped_user}:{escaped_password}@{host}:{port}/{database}"
            ),
            credentials=UserPasswordCredentials(user, password),
        )
        return config
    else:
        name = os.environ.get(
            f"{prefix}__POSTGRES_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
        )
        port = None
        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            port = create_docker_postgres_database(name)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / "docker_postgresql_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                port = create_docker_postgres_database(name)
        config = PostgresConfig(
            host=DB_HOST,
            port=int(port),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            uri=f"postgres://{DB_HOST}:{port}/{DB_NAME}",
            uri_with_credentials=(
                f"postgres://{urllib.parse.quote(DB_USER)}:"
                f"{urllib.parse.quote(DB_PASSWORD)}@{DB_HOST}:{port}/{DB_NAME}"
            ),
            credentials=UserPasswordCredentials(DB_USER, DB_PASSWORD),
        )
        return config


def create_docker_postgres_database(
    name: str = DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
):
    # noinspection PyPackageRequirements
    import docker  # Lazy import

    logger.info("Starting Postgres container")
    client = docker.from_env()
    if containers := client.containers.list(filters={"name": name}):
        logger.info("Postgres container already exists")
        container = containers[0]
        return get_container_port_binding(container, POSTGRES_FORWARDED_PORT)
    else:
        container = client.containers.run(
            "postgres:17.2",
            name=name,
            environment=[
                f"POSTGRES_DB={DB_NAME}",
                f"POSTGRES_USER={DB_USER}",
                f"POSTGRES_PASSWORD={DB_PASSWORD}",
            ],
            ports={POSTGRES_FORWARDED_PORT: None},
            detach=True,
        )
        container.reload()
        port = get_container_port_binding(container, POSTGRES_FORWARDED_PORT)
        # Wait for the database to be ready
        retry = 0
        while True:
            connection_params = {
                "dbname": DB_NAME,
                "user": DB_USER,
                "password": DB_PASSWORD,
                "host": DB_HOST,
                "port": str(port),
            }
            try:
                mydb = psycopg2.connect(**connection_params)
                break
            except Exception as err:
                retry += 1
                if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
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
        mycursor.execute("CREATE SCHEMA IF NOT EXISTS type_check_test_schema")
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS type_check_test_schema.all_postgres_types (
                id SERIAL PRIMARY KEY,
                c_smallint SMALLINT,
                c_integer INTEGER,
                c_bigint BIGINT,
                c_decimal DECIMAL(10,2),
                c_numeric NUMERIC(10,2),
                c_real REAL,
                c_double DOUBLE PRECISION,
                c_money MONEY,
                c_char CHAR(10),
                c_varchar VARCHAR(50),
                c_text TEXT,
                c_bytea BYTEA,
                c_boolean BOOLEAN,
                c_date DATE,
                c_time TIME,
                c_timetz TIME WITH TIME ZONE,
                c_timestamp TIMESTAMP,
                c_timestamptz TIMESTAMP WITH TIME ZONE,
                c_interval INTERVAL
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS type_check_test_schema.all_postgres_dest_types (
                id SERIAL PRIMARY KEY,
                c_smallint SMALLINT,
                c_integer INTEGER,
                c_bigint BIGINT,
                c_decimal DECIMAL(10,2),
                c_numeric NUMERIC(10,2),
                c_real REAL,
                c_double DOUBLE PRECISION,
                c_money MONEY,
                c_char CHAR(10),
                c_varchar VARCHAR(50),
                c_text TEXT,
                c_bytea BYTEA,
                c_boolean BOOLEAN,
                c_date DATE,
                c_time TIME,
                c_timetz TIME WITH TIME ZONE,
                c_timestamp TIMESTAMP,
                c_timestamptz TIMESTAMP WITH TIME ZONE,
                c_interval INTERVAL
            )
        """)
        sql = """
            INSERT INTO type_check_test_schema.all_postgres_types (
                c_smallint, c_integer, c_bigint, c_decimal, c_numeric, c_real, c_double,
                c_money, c_char, c_varchar, c_text, c_bytea, c_boolean, c_date, c_time, 
                c_timetz, c_timestamp, c_timestamptz, c_interval
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """
        val = (
            1,
            42,
            1234567890123,
            12345.67,
            23456.78,
            3.14,
            2.71828,
            100.50,
            "char10",
            "varchar50",
            "some text",
            b"\\xDEADBEEF",
            True,
            "2024-01-01",
            "12:34:56",
            "13:45:01+02",
            "2024-01-01 12:34:56",
            "2024-01-01 12:34:56+02",
            "1 year 2 months 3 days",
        )
        mycursor.execute(sql, val)
        mycursor.execute(
            "CREATE TABLE output_postgres_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_postgres_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            'CREATE TABLE output_new_postgres_transaction ("Duration" INT, "Pulse" INT,'
            ' "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE second_output_new_postgres_transaction ("Duration" INT,'
            ' "Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE output_new_postgres_wo_transaction ("Duration" INT, "Pulse"'
            ' INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute(
            'CREATE TABLE second_output_new_postgres_wo_transaction ("Duration" INT,'
            ' "Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
        )
        mycursor.execute("CREATE SCHEMA testing_schema")
        mydb.commit()
        logger.info("Postgres container created successfully")
        return port
