#
# Copyright 2026 Tabs Data Inc.
#

"""Oracle fixtures and utilities."""

import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from time import sleep
from zoneinfo import ZoneInfo

import oracledb
import pytest
from filelock import FileLock

from tabsdata._credentials import Credentials, UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_PASSWORD,
    MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}c_oracle"
)
ORACLE_PORT = 1521
ORACLE_FORWARDED_PORT = "1521/tcp"
ORACLE_DB_NAME = "FREE"
ORACLE_DB_USER = "system"

VALID_ORACLE_PREFIXES = ["ORACLE0"]


@dataclass
class OracleConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    uri: str
    uri_with_credentials: str
    credentials: Credentials


@pytest.fixture(scope="function")
def oracle_config(tmp_path_factory, worker_id, request) -> OracleConfig:
    marker = request.node.get_closest_marker("oracle")
    prefix = marker.kwargs.get("env_prefix", "ORACLE0") if marker else "ORACLE0"
    if prefix not in VALID_ORACLE_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are:"
            f" {', '.join(VALID_ORACLE_PREFIXES)}. If you want to extend the list of"
            " valid prefixes, please modify the 'VALID_ORACLE_PREFIXES' variable in"
            " the 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}__ORACLE_ENVS") in TRUE_VALUES:
        uri = os.environ.get(f"{prefix}__ORACLE_URI")
        if not uri:
            raise Exception(
                f"The environment variable {prefix}__ORACLE_URI is not set. Unable to"
                " run tests using the 'oracle_config' fixture."
            )
        pattern = r"^oracle://([^:/]+):(\d+)/(.*)$"
        match = re.match(pattern, uri)
        if not match:
            raise Exception(
                f"The ORACLE URI '{uri}' is not valid. Unable to run tests using the "
                "'oracle_config' fixture."
            )
        host, port, _ = match.groups()
        user = os.environ.get(f"{prefix}__ORACLE_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__ORACLE_USER is not set. Unable"
                " to run tests using the 'oracle_config' fixture."
            )
        password = os.environ.get(f"{prefix}__ORACLE_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__ORACLE_PASSWORD is not set."
                " Unable to run tests using the 'oracle_config' fixture."
            )
        database = os.environ.get(f"{prefix}__ORACLE_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__ORACLE_DATABASE is not set."
                " Unable to run tests using the 'oracle_config' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = OracleConfig(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            uri=uri,
            uri_with_credentials=(
                f"oracle://{escaped_user}:{escaped_password}@{host}:{port}/{database}"
            ),
            credentials=UserPasswordCredentials(user, password),
        )
        return config
    else:
        name = os.environ.get(
            f"{prefix}__ORACLE_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
        )
        port = None
        if worker_id == "master":
            port = create_docker_oracle_database(name)
        else:
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            fn = root_tmp_dir / "docker_oracle_creation"
            with FileLock(str(fn) + ".lock"):
                port = create_docker_oracle_database(name)
        config = OracleConfig(
            host=DB_HOST,
            port=int(port),
            user=ORACLE_DB_USER,
            password=DB_PASSWORD,
            database=ORACLE_DB_NAME,
            uri=f"oracle://{DB_HOST}:{port}/{ORACLE_DB_NAME}",
            uri_with_credentials=(
                f"oracle://{urllib.parse.quote(ORACLE_DB_USER)}:"
                f"{urllib.parse.quote(DB_PASSWORD)}@{DB_HOST}:{port}/{ORACLE_DB_NAME}"
            ),
            credentials=UserPasswordCredentials(ORACLE_DB_USER, DB_PASSWORD),
        )
        return config


def create_docker_oracle_database(
    name: str = DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
):
    # noinspection PyPackageRequirements
    import docker  # Lazy import

    logger.info("Starting Oracle container")
    client = docker.from_env()
    if containers := client.containers.list(filters={"name": name}):
        logger.info("Oracle container already exists")
        container = containers[0]
        return get_container_port_binding(container, ORACLE_FORWARDED_PORT)
    else:
        container = client.containers.run(
            "container-registry.oracle.com/database/free:23.6.0.0-lite",
            name=name,
            environment=[
                f"ORACLE_PWD={DB_PASSWORD}",
            ],
            ports={ORACLE_FORWARDED_PORT: None},
            detach=True,
        )
        container.reload()
        port = get_container_port_binding(container, ORACLE_FORWARDED_PORT)
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                dsn_tns = oracledb.makedsn(DB_HOST, port, service_name="FREE")
                mydb = oracledb.connect(
                    user="system", password=DB_PASSWORD, dsn=dsn_tns
                )
                break
            except (Exception, oracledb.DatabaseError) as err:
                retry += 1
                # Waiting for up to 10'' & 30'', as young Gauss already knew.
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
        mycursor.execute("""
            CREATE TABLE all_oracle_types (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                c_number NUMBER,
                c_float FLOAT,
                c_binary_float BINARY_FLOAT,
                c_binary_double BINARY_DOUBLE,
                c_char CHAR(10),
                c_varchar2 VARCHAR2(50),
                c_nchar NCHAR(10),
                c_nvarchar2 NVARCHAR2(50),
                c_date DATE,
                c_timestamp TIMESTAMP,
                c_timestamp_tz TIMESTAMP WITH TIME ZONE,
                c_timestamp_ltz TIMESTAMP WITH LOCAL TIME ZONE,
                c_interval_ym INTERVAL YEAR TO MONTH,
                c_interval_ds INTERVAL DAY TO SECOND,
                c_clob CLOB,
                c_nclob NCLOB,
                c_blob BLOB,
                c_raw RAW(16),
                c_long LONG
            )
        """)

        sql = """
            INSERT INTO all_oracle_types (
                c_number, c_float, c_binary_float, c_binary_double, c_char, c_varchar2,
                c_nchar, c_nvarchar2, c_date, c_timestamp, c_timestamp_tz, c_timestamp_ltz,
                c_interval_ym, c_interval_ds, c_clob, c_nclob, c_blob, c_raw, c_long
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19
            )
        """
        val = (
            42,
            3.14,
            1.23,
            2.34,
            "char10",
            "varchar50",
            "nchar10",
            "nvarchar50",
            date(2024, 1, 1),  # c_date
            datetime(2024, 1, 1, 13, 0, 0),  # c_timestamp
            datetime(
                2024, 1, 1, 12, 34, 56, tzinfo=ZoneInfo("Australia/Sydney")
            ),  # c_timestamp_tz
            datetime(2024, 1, 1, 12, 34, 56, tzinfo=ZoneInfo("Australia/Sydney")),
            "2-6",  # INTERVAL YEAR TO MONTH
            "5 12:30:16.123",  # INTERVAL DAY TO SECOND
            "some clob data",
            "some nclob data",
            b"some blob data",
            b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            "some long data",
        )
        mycursor.execute(sql, val)
        mydb.commit()
        logger.info("Oracle container created successfully")
        return port
