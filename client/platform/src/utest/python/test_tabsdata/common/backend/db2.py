#
# Copyright 2026 Tabs Data Inc.
#

"""DB2 fixtures and utilities."""

import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from time import sleep

import pytest
from filelock import FileLock

from tabsdata._credentials import Credentials, UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_PASSWORD,
    CONTAINER_MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_DB2_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}db2"
)
DB2_PORT = 50000
DB2_FORWARDED_PORT = "50000/tcp"
DB2_DB_NAME = "testdb"
DB2_DB_USER = "db2inst1"

VALID_DB2_PREFIXES = ["DB20"]


@dataclass
class DB2Config:
    host: str
    port: int
    user: str
    password: str
    database: str
    uri: str
    uri_with_credentials: str
    credentials: Credentials


@pytest.fixture(scope="function")
def db2_config(tmp_path_factory, worker_id, request) -> DB2Config:
    marker = request.node.get_closest_marker("db2")
    prefix = marker.kwargs.get("env_prefix", "DB20") if marker else "DB20"
    port = -1
    if prefix not in VALID_DB2_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are:"
            f" {', '.join(VALID_DB2_PREFIXES)}. If you want to extend the list of"
            " valid prefixes, please modify the 'VALID_DB2_PREFIXES' variable in"
            " the 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}__DB2_ENVS") in TRUE_VALUES:
        uri = os.environ.get(f"{prefix}__DB2_URI")
        if not uri:
            raise Exception(
                f"The environment variable {prefix}__DB2_URI is not set. Unable to"
                " run tests using the 'db2_config' fixture."
            )
        pattern = r"^db2://([^:/]+):(\d+)/(.*)$"
        match = re.match(pattern, uri)
        if not match:
            raise Exception(
                f"The DB2 URI '{uri}' is not valid. Unable to run tests using the "
                "'db2_config' fixture."
            )
        host, port, _ = match.groups()
        user = os.environ.get(f"{prefix}__DB2_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__DB2_USER is not set. Unable"
                " to run tests using the 'db2_config' fixture."
            )
        password = os.environ.get(f"{prefix}__DB2_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__DB2_PASSWORD is not set."
                " Unable to run tests using the 'db2_config' fixture."
            )
        database = os.environ.get(f"{prefix}__DB2_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__DB2_DATABASE is not set."
                " Unable to run tests using the 'db2_config' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = DB2Config(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            uri=uri,
            uri_with_credentials=(
                f"db2://{escaped_user}:{escaped_password}@{host}:{port}/{database}"
            ),
            credentials=UserPasswordCredentials(user, password),
        )
        return config
    else:
        name = os.environ.get(
            f"{prefix}__DB2_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_DB2_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            port = create_docker_db2_database(name)
        else:
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            fn = root_tmp_dir / "docker_db2_creation"
            with FileLock(str(fn) + ".lock"):
                port = create_docker_db2_database(name)
        config = DB2Config(
            host=DB_HOST,
            port=int(port),
            user=DB2_DB_USER,
            password=DB_PASSWORD,
            database=DB2_DB_NAME,
            uri=f"db2://{DB_HOST}:{port}/{DB2_DB_NAME}",
            uri_with_credentials=(
                f"db2://{urllib.parse.quote(DB2_DB_USER)}:"
                f"{urllib.parse.quote(DB_PASSWORD)}@{DB_HOST}:{port}/{DB2_DB_NAME}"
            ),
            credentials=UserPasswordCredentials(DB2_DB_USER, DB_PASSWORD),
        )
        return config


def wait_for_db2_ready(port):
    import ibm_db_dbi as db

    retry = 0
    while True:
        try:
            connection = db.connect(
                f"DATABASE={DB2_DB_NAME};"
                f"HOSTNAME=127.0.0.1;"
                f"PORT={port};"
                f"PROTOCOL=TCPIP;",
                user=DB2_DB_USER, password=DB_PASSWORD,
            )
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            cursor.close()
            connection.close()
            logger.info(f"DB2 readiness check passed on port {port}")
            return
        except Exception as err:
            retry += 1
            if retry == CONTAINER_MAXIMUM_RETRY_COUNT:
                logger.error(f"DB2 readiness check failed after {retry} retries: {err}")
                raise err
            else:
                logger.warning(
                    f"DB2 readiness check failed, retrying in {retry} second(s): {err}"
                )
                sleep(retry)


def create_docker_db2_database(
    name: str = DEFAULT_PYTEST_DB2_DOCKER_CONTAINER_NAME,
):
    # noinspection PyPackageRequirements
    import docker
    import ibm_db_dbi as db

    logger.info("Starting DB2 container")
    client = docker.from_env()
    if containers := client.containers.list(filters={"name": name}):
        logger.info("DB2 container already exists")
        container = containers[0]
        port = get_container_port_binding(container, DB2_FORWARDED_PORT)
        wait_for_db2_ready(port)
        return port
    else:
        container = client.containers.run(
            "icr.io/db2_community/db2:latest",
            name=name,
            platform="linux/amd64",
            environment=[
                f"DB2INST1_PASSWORD={DB_PASSWORD}",
                "LICENSE=accept",
                f"DBNAME={DB2_DB_NAME}",

            ],
            ports={DB2_FORWARDED_PORT: None},
            detach=True,
            privileged=True,
        )
        container.reload()
        port = get_container_port_binding(container, DB2_FORWARDED_PORT)
        wait_for_db2_ready(port)
        mydb = db.connect(
            f"DATABASE={DB2_DB_NAME};"
            f"HOSTNAME=127.0.0.1;"
            f"PORT={port};"
            f"PROTOCOL=TCPIP;",
            user=DB2_DB_USER, password=DB_PASSWORD,
        )
        try:
            mycursor = mydb.cursor()
            try:
                mycursor.execute(
                    "CREATE TABLE INVOICE_HEADER (id INT GENERATED BY DEFAULT"
                    " AS IDENTITY, name VARCHAR(255))"
                )
                names = [
                    "Arvind", "Tucu", "Dimas", "Joaquin", "Jennifer", "Aleix",
                ]
                for name in names:
                    sql = f"INSERT INTO INVOICE_HEADER (name) VALUES ('{name}')"
                    mycursor.execute(sql)
                mycursor.execute(
                    "CREATE TABLE INVOICE_ITEM (id INT GENERATED BY DEFAULT"
                    " AS IDENTITY, name VARCHAR(255))"
                )
                names = [
                    "Leonardo", "Donatello", "Michelangelo", "Raphael",
                    "Splinter",
                ]
                for name in names:
                    sql = f"INSERT INTO INVOICE_ITEM (name) VALUES ('{name}')"
                    mycursor.execute(sql)
                mycursor.execute(
                    'CREATE TABLE output_db2_list ("Duration" INT, '
                    '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
                )
                mycursor.execute(
                    'CREATE TABLE output_db2_driver_provided ("Duration" INT, '
                    '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
                )
                mycursor.execute(
                    'CREATE TABLE second_output_db2_list ("Duration" INT, '
                    '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
                )
                mycursor.execute(
                    'CREATE TABLE output_db2_transaction ("Duration" INT, '
                    '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
                )
                mycursor.execute(
                    'CREATE TABLE second_output_db2_transaction ("Duration" INT, '
                    '"Pulse" INT, "Maxpulse" INT, "Calories" FLOAT)'
                )
                mycursor.execute("""
                    CREATE TABLE all_db2_types (
                        id          INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        c_smallint  SMALLINT,
                        c_int       INTEGER,
                        c_bigint    BIGINT,
                        c_decimal   DECIMAL(10, 2),
                        c_real      REAL,
                        c_double    DOUBLE,
                        c_char      CHAR(10),
                        c_varchar   VARCHAR(50),
                        c_binary    CHAR(16) FOR BIT DATA,
                        c_varbinary VARCHAR(50) FOR BIT DATA,
                        c_date      DATE,
                        c_time      TIME,
                        c_timestamp TIMESTAMP,
                        c_clob      CLOB,
                        c_blob      BLOB,
                        c_dbclob    DBCLOB,
                        c_xml       XML
                    )
                """)

                sql = """
                    INSERT INTO all_db2_types (
                        c_smallint, c_int, c_bigint, c_decimal, c_real, c_double,
                        c_char, c_varchar, c_binary, c_varbinary,
                        c_date, c_time, c_timestamp,
                        c_clob, c_blob, c_dbclob, c_xml
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                """
                val = (
                    32767,  # c_smallint
                    42,  # c_int
                    9999999999,  # c_bigint
                    3.14,  # c_decimal
                    1.23,  # c_real
                    2.34,  # c_double
                    "char10",  # c_char
                    "varchar50",  # c_varchar
                    b"\x00\x11\x22\x33\x44\x55\x66\x77",  # c_binary
                    b"\x88\x99\xaa\xbb\xcc\xdd\xee\xff",  # c_varbinary
                    date(2024, 1, 1),  # c_date
                    "13:00:00",  # c_time
                    datetime(2024, 1, 1, 13, 0, 0),  # c_timestamp
                    "some clob data",  # c_clob
                    b"some blob data",  # c_blob
                    "some dbclob data",  # c_dbclob
                    "<root><value>42</value></root>",  # c_xml
                )
                mycursor.execute(sql, val)
                mydb.commit()
            finally:
                mycursor.close()
        finally:
            mydb.close()
        logger.info("DB2 container created successfully")
        return port
