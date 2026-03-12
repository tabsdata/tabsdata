#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import random
import string
from dataclasses import dataclass
from typing import Union

import mysql.connector
import pytest
import docker
from docker.models.containers import Container
from filelock import FileLock
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent

from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import DB_HOST, DB_PASSWORD
from test_tabsdata_cdc_common.common.docker import (
    CDC_CONTAINER_PREFIX,
    CDC_DATABASE,
    CDC_PASSWORD,
    CDC_USER,
    DOCKER_LOCK_SUFFIX,
    PYTEST_XDIST_MASTER,
    ensure_connection,
    ensure_container,
    obtain_environment,
    obtain_prefix,
)
from test_tabsdata_cdc_mysql.common.db import server_id

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_MYSQL_CDC_DOCKER_CONTAINER_NAME: str = f"{CDC_CONTAINER_PREFIX}mysql-cdc"

MYSQL_CDC_FORWARDED_PORT: str = "3306/tcp"
MYSQL_CDC_IMAGE: str = "mysql:9.1.0"

MYSQL_CDC_DATABASE: str = CDC_DATABASE
MYSQL_CDC_ROOT_USER: str = "root"
MYSQL_CDC_ROOT_PASSWORD: str = DB_PASSWORD
MYSQL_CDC_USER: str = CDC_USER
MYSQL_CDC_PASSWORD: str = CDC_PASSWORD

VALID_MYSQL_CDC_PREFIXES: list[str] = ["MYSQLCDC0"]
MYSQL_CDC_DEFAULT_PREFIX: str = "MYSQLCDC0"
MYSQL_CDC_LOCK: str = "docker_cdc_mysql_creation"
MYSQL_CDC_MARKER: str = "cdc_mysql"
MYSQL_CDC_ENV_SUFFIX: str = "MYSQLCDC"
MYSQL_CDC_REQUIRED_ENVS: list[str] = [
    "HOST",
    "PORT",
    "DATABASE",
    "ROOT_USER",
    "ROOT_PASSWORD",
    "CDC_USER",
    "CDC_PASSWORD",
]

MYSQL_CDC_SIGNAL_TABLE: str = "td_cdc_signal"


@dataclass
class MySQLCdcConfig:
    container: Container
    host: str
    port: int
    database: str
    root_user: str
    root_password: str
    cdc_user: str
    cdc_password: str


def mysql_conn(port: int) -> Union[PooledMySQLConnection, MySQLConnectionAbstract]:
    return mysql.connector.connect(
        host=DB_HOST,
        port=port,
        database=MYSQL_CDC_DATABASE,
        user=MYSQL_CDC_ROOT_USER,
        password=MYSQL_CDC_ROOT_PASSWORD,
        use_pure=True,
    )


def setup_cdc_mysql_user(port: int):
    connection = None
    cursor = None
    try:
        connection = mysql_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"create user if not exists '{MYSQL_CDC_USER}'@'%' "
            f"identified by '{MYSQL_CDC_PASSWORD}'"
        )
        cursor.execute(
            f"grant "
            f"replication slave, "
            f"replication client "
            f"on *.* "
            f"to '{MYSQL_CDC_USER}'@'%'"
        )
        cursor.execute(
            f"grant select on {MYSQL_CDC_DATABASE}.* to '{MYSQL_CDC_USER}'@'%'"
        )
        cursor.execute(f"grant reload on *.* to '{MYSQL_CDC_USER}'@'%'")
        cursor.execute(
            f"grant lock tables on {MYSQL_CDC_DATABASE}.* to '{MYSQL_CDC_USER}'@'%'"
        )
        cursor.execute("flush privileges")
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info("Setting user for cdc completed successfully")


def setup_cdc_mysql_signal(port: int) -> str:
    table_suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    table_name = f"{MYSQL_CDC_SIGNAL_TABLE}_{table_suffix}"

    connection = None
    cursor = None
    try:
        connection = mysql_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"create table if not exists {table_name} "
            "("
            "id int auto_increment primary key, "
            "name varchar(255), "
            "code int"
            ")"
        )
        for name, code in [("Go Seigen", 21), ("Lee Sedol", 17), ("Kitani Minoru", 19)]:
            cursor.execute(
                f"insert into {table_name} (name, code) values ('{name}', {code})"
            )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info(f"Setup of the signal table '{table_name}' completed successfully")

    return table_name


def check_cdc_mysql_binlog_configuration(port: int, signal: str) -> None:
    connection = None
    cursor = None
    try:
        connection = mysql_conn(port)
        cursor = connection.cursor()

        cursor.execute("show variables like 'log_bin'")
        row = cursor.fetchone()
        if not row or row[1] != "ON":
            raise RuntimeError(f"CDC check failed: log_bin is not ON (got {row})")

        cursor.execute("show variables like 'binlog_format'")
        row = cursor.fetchone()
        if not row or row[1] != "ROW":
            raise RuntimeError(
                f"CDC check failed: binlog_format is not ROW (got {row})"
            )

        cursor.execute("show variables like 'binlog_row_image'")
        row = cursor.fetchone()
        if not row or row[1] != "FULL":
            raise RuntimeError(
                f"CDC check failed: binlog_row_image is not FULL (got {row})"
            )

        cursor.execute("show variables like 'binlog_row_metadata'")
        row = cursor.fetchone()
        if not row or row[1] != "FULL":
            raise RuntimeError(
                f"CDC check failed: binlog_row_metadata is not FULL (got {row})"
            )

        cursor.execute("show variables like 'gtid_mode'")
        row = cursor.fetchone()
        if not row or row[1] != "ON":
            raise RuntimeError(f"CDC check failed: gtid_mode is not ON (got {row})")

        cursor.execute("show binary log status")
        row = cursor.fetchone()
        if not row:
            raise RuntimeError(
                "CDC check failed: show binary log status returned no rows"
            )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_mysql_binlog_operation_events(port: int, signal: str) -> None:
    connection = None
    cursor = None
    try:
        connection = mysql_conn(port)
        cursor = connection.cursor()

        cursor.execute("show binary log status")
        row = cursor.fetchone()
        if not row:
            raise RuntimeError(
                "CDC check failed: show binary log status returned no rows"
            )

        cursor.execute(f"select count(*) from {signal}")
        count = cursor.fetchone()[0]
        if count != 3:
            raise RuntimeError(
                f"CDC check failed: signal table '{signal}' "
                f"has {count} rows, expected 3"
            )

        binlog_file = row[0]
        cursor.execute(
            f"show binlog events in '{binlog_file}'"
        )
        events = cursor.fetchall()
        signal_events = [
            event for event in events
            if len(event) > 5 and signal in str(event[5])
        ]
        if not signal_events:
            raise RuntimeError(
                f"CDC check failed: no binlog events found "
                f"for signal table '{signal}'"
            )

        logger.info(
            f"MySQL CDC binlog verification passed "
            f"(3 rows in '{signal}', "
            f"{len(signal_events)} binlog events)"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_mysql_binlog_operation_stream(port: int, signal: str) -> None:
    expected_rows = [
        {"id": 1, "name": "Go Seigen", "code": 21},
        {"id": 2, "name": "Lee Sedol", "code": 17},
        {"id": 3, "name": "Kitani Minoru", "code": 19},
    ]

    connection = None
    cursor = None
    try:
        connection = mysql_conn(port)
        cursor = connection.cursor()
        cursor.execute("show binary logs")
        first_log = cursor.fetchall()[0][0]
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    stream = None
    try:
        stream = BinLogStreamReader(
            connection_settings={
                "host": DB_HOST,
                "port": port,
                "user": MYSQL_CDC_ROOT_USER,
                "passwd": MYSQL_CDC_ROOT_PASSWORD,
            },
            server_id=server_id(),
            only_events=[WriteRowsEvent],
            only_tables=[signal],
            only_schemas=[MYSQL_CDC_DATABASE],
            log_file=first_log,
            log_pos=4,
            blocking=False,
        )
        captured_rows = []
        for event in stream:
            for row in event.rows:
                captured_rows.append(row["values"])
    finally:
        stream.close()

    if len(captured_rows) != len(expected_rows):
        raise RuntimeError(
            f"CDC check failed: expected {len(expected_rows)} "
            f"WriteRowsEvents for '{signal}', "
            f"found {len(captured_rows)}"
        )

    for expected, captured in zip(expected_rows, captured_rows):
        if captured != expected:
            raise RuntimeError(
                f"CDC check failed: expected row {expected}, "
                f"got {captured}"
            )

    logger.info(
        f"MySQL CDC binlog data verification passed "
        f"({len(captured_rows)} rows with correct values "
        f"captured from binlog for '{signal}')"
    )


def create_cdc_mysql_container(
    name: str = DEFAULT_PYTEST_MYSQL_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int, bool]:
    container, port, created = ensure_container(
        name=name,
        image=MYSQL_CDC_IMAGE,
        environment=[
            f"MYSQL_ROOT_PASSWORD={MYSQL_CDC_ROOT_PASSWORD}",
            f"MYSQL_DATABASE={MYSQL_CDC_DATABASE}",
            f"MYSQL_USER={MYSQL_CDC_USER}",
            f"MYSQL_PASSWORD={MYSQL_CDC_PASSWORD}",
        ],
        forwarded_ports={MYSQL_CDC_FORWARDED_PORT: None},
        command=[
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog_format=ROW",
            "--binlog_row_image=FULL",
            "--binlog_row_metadata=FULL",
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--binlog_expire_logs_seconds=604800",
        ],
    )
    return container, port, created


def await_cdc_mysql_container(port: int) -> None:

    def connect() -> None:
        connection = None
        cursor = None
        try:
            connection = mysql_conn(port)
            cursor = connection.cursor()
            cursor.execute("select 1 from dual")
            cursor.fetchone()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    ensure_connection(connect, "MySQL CDC")


def setup_cdc_mysql_container(port: int) -> str:
    setup_cdc_mysql_user(port)
    signal = setup_cdc_mysql_signal(port)

    return signal


def check_cdc_mysql_container(port: int, signal: str) -> None:
    check_cdc_mysql_binlog_configuration(port, signal)
    check_cdc_mysql_binlog_operation_events(port, signal)
    check_cdc_mysql_binlog_operation_stream(port, signal)


def create_cdc_mysql_database(
    name: str = DEFAULT_PYTEST_MYSQL_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int]:
    container, port, created = create_cdc_mysql_container(name)
    await_cdc_mysql_container(port)
    if created:
        signal = setup_cdc_mysql_container(port)
        check_cdc_mysql_container(port, signal)

    return container, port


@pytest.fixture(scope="function")
def mysql_cdc_config(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> MySQLCdcConfig:
    prefix = obtain_prefix(
        request,
        MYSQL_CDC_MARKER,
        MYSQL_CDC_DEFAULT_PREFIX,
        VALID_MYSQL_CDC_PREFIXES,
    )
    env_prefix = f"{prefix}__{MYSQL_CDC_ENV_SUFFIX}"
    if os.environ.get(f"{env_prefix}_ENVS") in TRUE_VALUES:
        envs = obtain_environment(
            env_prefix, MYSQL_CDC_REQUIRED_ENVS,
            "mysql_cdc_config",
        )
        port = int(envs["PORT"])
        container = None
        client = docker.from_env()
        for c in client.containers.list():
            bindings = (
                c.attrs.get("NetworkSettings", {})
                .get("Ports", {})
                .get(MYSQL_CDC_FORWARDED_PORT, [])
            ) or []
            for binding in bindings:
                if int(binding.get("HostPort", 0)) == port:
                    container = c
                    break
            if container is not None:
                break
        return MySQLCdcConfig(
            container=container,
            host=envs["HOST"],
            port=port,
            database=envs["DATABASE"],
            root_user=envs["ROOT_USER"],
            root_password=envs["ROOT_PASSWORD"],
            cdc_user=envs["CDC_USER"],
            cdc_password=envs["CDC_PASSWORD"],
        )
    else:
        name = os.environ.get(
            f"{env_prefix}_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_MYSQL_CDC_DOCKER_CONTAINER_NAME,
        )
        container = None
        port = -1
        if worker_id == PYTEST_XDIST_MASTER:
            container, port = create_cdc_mysql_database(name)
        else:
            root_temp_dir = tmp_path_factory.getbasetemp().parent
            lock_file = root_temp_dir / MYSQL_CDC_LOCK
            with FileLock(str(lock_file) + DOCKER_LOCK_SUFFIX):
                container, port = create_cdc_mysql_database(name)
        return MySQLCdcConfig(
            container=container,
            host=DB_HOST,
            port=int(port),
            database=MYSQL_CDC_DATABASE,
            root_user=MYSQL_CDC_USER,
            root_password=MYSQL_CDC_PASSWORD,
            cdc_user=MYSQL_CDC_USER,
            cdc_password=MYSQL_CDC_PASSWORD,
        )
