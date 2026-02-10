#
# Copyright 2026 Tabs Data Inc.
#

"""MySQL fixtures and utilities."""

import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from time import sleep

import mysql.connector
import pytest
from filelock import FileLock

from tabsdata._credentials import Credentials, UserPasswordCredentials
from tabsdata._utils.constants import TRUE_VALUES
from src.utest.python.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from src.utest.python.common.container import get_container_port_binding

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}c_mysql"
)
MYSQL_FORWARDED_PORT = "3306/tcp"

VALID_MYSQL_PREFIXES = ["MYSQL0"]


@dataclass
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    uri: str
    uri_with_credentials: str
    credentials: Credentials


@pytest.fixture(scope="function")
def mysql_config(tmp_path_factory, worker_id, request) -> MySQLConfig:
    marker = request.node.get_closest_marker("mysql")
    prefix = marker.kwargs.get("env_prefix", "MYSQL0") if marker else "MYSQL0"
    if prefix not in VALID_MYSQL_PREFIXES:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(VALID_MYSQL_PREFIXES)}. If you want to extend the list of "
            "valid prefixes, please modify the 'VALID_MYSQL_PREFIXES' variable in the"
            " 'conftest.py' file."
        )
    if os.environ.get(f"{prefix}__MYSQL_ENVS") in TRUE_VALUES:
        uri = os.environ.get(f"{prefix}__MYSQL_URI")
        if not uri:
            raise Exception(
                f"The environment variable {prefix}__MYSQL_URI is not set. Unable to "
                "run "
                "tests using the 'mysql_config' fixture."
            )
        pattern = (
            r"^mysql://(?:([^:@/]+)(?::([^@/]*))?@)?([^:/]+):(\d+)(?:/([^/?#]+))?$"
        )
        match = re.match(pattern, uri)
        if not match:
            raise Exception(
                f"The MYSQL URI '{uri}' is not valid. Unable to run tests using the "
                "'mysql_config' fixture."
            )
        _, _, host, port, _ = match.groups()
        user = os.environ.get(f"{prefix}__MYSQL_USER")
        if not user:
            raise Exception(
                f"The environment variable {prefix}__MYSQL_USER is not set and the "
                f"user is not part of the {prefix}__MYSQL_URI. Unable to "
                "run tests using the 'mysql_config' fixture."
            )
        password = os.environ.get(f"{prefix}__MYSQL_PASSWORD")
        if not password:
            raise Exception(
                f"The environment variable {prefix}__MYSQL_PASSWORD is not set and "
                f"the password is not part of the {prefix}__MYSQL_URI. Unable to "
                "run tests using the 'mysql_config' fixture."
            )
        database = os.environ.get(f"{prefix}__MYSQL_DATABASE")
        if not database:
            raise Exception(
                f"The environment variable {prefix}__MYSQL_DATABASE is not set and "
                f"the database is not part of the {prefix}__MYSQL_URI. Unable to "
                "run tests using the 'mysql_config' fixture."
            )
        escaped_user = urllib.parse.quote(user)
        escaped_password = urllib.parse.quote(password)
        config = MySQLConfig(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            uri=uri,
            uri_with_credentials=(
                f"mysql://{escaped_user}:{escaped_password}@{host}:{port}/{database}"
            ),
            credentials=UserPasswordCredentials(user, password),
        )
        return config
    else:
        port = None
        name = os.environ.get(
            f"{prefix}__MYSQL_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
        )
        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            port = create_docker_mysql_database(name)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / "docker_mysql_creation"
            with FileLock(str(fn) + ".lock"):
                # only one worker will be able to create the database
                port = create_docker_mysql_database(name)
        config = MySQLConfig(
            host=DB_HOST,
            port=int(port),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            uri=f"mysql://{DB_HOST}:{port}/{DB_NAME}",
            uri_with_credentials=(
                f"mysql://{urllib.parse.quote(DB_USER)}:"
                f"{urllib.parse.quote(DB_PASSWORD)}@{DB_HOST}:{port}/{DB_NAME}"
            ),
            credentials=UserPasswordCredentials(DB_USER, DB_PASSWORD),
        )
        return config


def create_docker_mysql_database(
    name: str = DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
):
    # noinspection PyPackageRequirements
    import docker  # Lazy import

    logger.info("Starting MySQL container")
    client = docker.from_env()
    if containers := client.containers.list(filters={"name": name}):
        logger.info("MySQL container already exists")
        container = containers[0]
        return get_container_port_binding(container, MYSQL_FORWARDED_PORT)
    else:
        container = client.containers.run(
            "mysql:9.1.0",
            name=name,
            environment=[
                "MYSQL_ROOT_PASSWORD=password",
                f"MYSQL_DATABASE={DB_NAME}",
                f"MYSQL_USER={DB_USER}",
                f"MYSQL_PASSWORD={DB_PASSWORD}",
            ],
            ports={MYSQL_FORWARDED_PORT: None},
            detach=True,
        )
        container.reload()
        port = get_container_port_binding(container, MYSQL_FORWARDED_PORT)
        # Wait for the database to be ready
        retry = 0
        while True:
            try:
                mydb = mysql.connector.connect(
                    host=DB_HOST,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    port=port,
                )
                break
            except mysql.connector.Error as err:
                retry += 1
                # Waiting for up to 10'' & 30'', as young Gauss already knew.
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
            "CREATE TABLE output_new_sql_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE output_new_sql_wo_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_sql_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_new_sql_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute(
            "CREATE TABLE second_output_new_sql_wo_transaction (Duration INT, "
            "Pulse INT, Maxpulse INT, Calories FLOAT)"
        )
        mycursor.execute("""
            CREATE TABLE ALL_MYSQL_TYPES (
                id INT AUTO_INCREMENT PRIMARY KEY,
                c_tinyint TINYINT,
                c_smallint SMALLINT,
                c_mediumint MEDIUMINT,
                c_int INT,
                c_bigint BIGINT,
                c_float FLOAT,
                c_double DOUBLE,
                c_decimal DECIMAL(10,2),
                c_char CHAR(10),
                c_varchar VARCHAR(50),
                c_binary BINARY(8),
                c_varbinary VARBINARY(16),
                c_tinyblob TINYBLOB,
                c_blob BLOB,
                c_mediumblob MEDIUMBLOB,
                c_longblob LONGBLOB,
                c_tinytext TINYTEXT,
                c_text TEXT,
                c_mediumtext MEDIUMTEXT,
                c_longtext LONGTEXT,
                c_enum ENUM('A', 'B', 'C'),
                c_set SET('X', 'Y', 'Z'),
                c_date DATE,
                c_datetime DATETIME,
                c_timestamp TIMESTAMP NULL,
                c_time TIME,
                c_year YEAR
            )
            """)
        sql = """
            INSERT INTO ALL_MYSQL_TYPES (
                c_tinyint, c_smallint, c_mediumint, c_int, c_bigint,
                c_float, c_double, c_decimal, c_char, c_varchar,
                c_binary, c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob,
                c_tinytext, c_text, c_mediumtext, c_longtext,
                c_enum, c_set, c_date, c_datetime, c_timestamp, c_time, c_year
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        val = [
            (
                0,
                2,
                1000,
                10000,
                100000,
                1.23,
                2.34,
                12345.67,
                "char1",
                "varchar1",
                b"binary1",
                b"varbinary1",
                b"tinyblob1",
                b"blob1",
                b"mediumblob1",
                b"longblob1",
                "tinytext1",
                "text1",
                "mediumtext1",
                "longtext1",
                "A",
                "X,Y",
                "2024-01-01",
                "2024-01-01 12:34:56",
                "2024-01-01 12:34:56",
                "12:34:56",
                2024,
            ),
            (
                0,
                200,
                2000,
                20000,
                200000,
                3.45,
                4.56,
                23456.78,
                "char2",
                "varchar2",
                b"binary2",
                b"varbinary2",
                b"tinyblob2",
                b"blob2",
                b"mediumblob2",
                b"longblob2",
                "tinytext2",
                "text2",
                "mediumtext2",
                "longtext2",
                "B",
                "Y,Z",
                "2025-02-02",
                "2025-02-02 23:45:01",
                "2025-02-02 23:45:01",
                "23:45:01",
                2025,
            ),
        ]
        mycursor.executemany(sql, val)
        mydb.commit()
        logger.info("MySQL container created successfully")
        return port
