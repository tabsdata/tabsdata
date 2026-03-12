#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import random
import string
from dataclasses import dataclass

import orjson
import psycopg2
import psycopg2.extensions
import pytest
from docker.models.containers import Container
from filelock import FileLock

from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import DB_HOST, DB_PASSWORD, DB_USER
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

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_POSTGRESQL_CDC_DOCKER_CONTAINER_NAME: str = (
    f"{CDC_CONTAINER_PREFIX}postgresql-cdc"
)

POSTGRESQL_CDC_FORWARDED_PORT: str = "5432/tcp"
POSTGRESQL_CDC_IMAGE: str = "postgres:17.2"

POSTGRESQL_CDC_DATABASE: str = CDC_DATABASE
POSTGRESQL_CDC_ROOT_USER: str = DB_USER
POSTGRESQL_CDC_ROOT_PASSWORD: str = DB_PASSWORD
POSTGRESQL_CDC_USER: str = CDC_USER
POSTGRESQL_CDC_PASSWORD: str = CDC_PASSWORD
POSTGRESQL_CDC_SLOT: str = "tdcdcdb_slot"

VALID_POSTGRESQL_CDC_PREFIXES: list[str] = ["POSTGRESQLCDC0"]
POSTGRESQL_CDC_DEFAULT_PREFIX: str = "POSTGRESQLCDC0"
POSTGRESQL_CDC_LOCK: str = "docker_cdc_postgresql_creation"
POSTGRESQL_CDC_MARKER: str = "cdc_postgresql"
POSTGRESQL_CDC_ENV_SUFFIX: str = "POSTGRESQLCDC"
POSTGRESQL_CDC_REQUIRED_ENVS: list[str] = [
    "HOST",
    "PORT",
    "DATABASE",
    "SLOT",
    "ROOT_USER",
    "ROOT_PASSWORD",
    "CDC_USER",
    "CDC_PASSWORD",
]

POSTGRESQL_CDC_SIGNAL_TABLE: str = "td_cdc_signal"


@dataclass
class PostgreSQLCDCConfig:
    host: str
    port: int
    database: str
    slot: str
    root_user: str
    root_password: str
    cdc_user: str
    cdc_password: str


def postgresql_conn(port: int) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST,
        port=str(port),
        dbname=POSTGRESQL_CDC_DATABASE,
        user=POSTGRESQL_CDC_ROOT_USER,
        password=POSTGRESQL_CDC_ROOT_PASSWORD,
    )


def postgresql_exec(container: Container, command: str, user: str | None = None) -> str:
    code, output = container.exec_run(
        ["bash", "-c", command],
        **({"user": user} if user else {}),
    )
    output = output.decode("utf-8", errors="replace")
    if code != 0:
        raise RuntimeError(f"PostgreSQL command failed:\n{command}\n\n{output}")
    return output


def setup_cdc_postgresql_wal2json(container: Container) -> None:
    postgresql_exec(
        container,
        "apt-get update && apt-get install -y postgresql-17-wal2json",
    )

    logger.info("Setting wal2json plugin completed successfully")

    postgresql_exec(
        container,
        "pg_ctl reload -D /var/lib/postgresql/data",
        user="postgres",
    )

    logger.info("Reload of PostgreSQL configuration completed successfully")


def setup_cdc_postgresql_user(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = postgresql_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(
            f"do $$ "
            f"begin "
            f"if not exists (select from pg_roles where rolname = "
            f"'{POSTGRESQL_CDC_USER}') then "
            f"create role {POSTGRESQL_CDC_USER} "
            f"with login password '{POSTGRESQL_CDC_PASSWORD}' "
            f"replication; "
            f"end if; "
            f"end $$"
        )

        cursor.execute(
            f"grant "
            f"connect on database {POSTGRESQL_CDC_DATABASE} "
            f"to {POSTGRESQL_CDC_USER}"
        )

        cursor.execute(f"grant usage on schema public to {POSTGRESQL_CDC_USER}")
        cursor.execute(
            f"grant select on all tables in schema public to {POSTGRESQL_CDC_USER}"
        )
        cursor.execute(
            f"alter default privileges in schema public "
            f"grant "
            f"select on tables "
            f"to {POSTGRESQL_CDC_USER}"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def setup_cdc_postgresql_signal(port: int) -> str:
    table_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    table_name = f"{POSTGRESQL_CDC_SIGNAL_TABLE}_{table_suffix}"

    connection = None
    cursor = None
    try:
        connection = postgresql_conn(port)
        cursor = connection.cursor()

        cursor.execute(
            f"create table if not exists {table_name} "
            f"("
            f"id serial primary key, "
            f"name varchar(255), "
            f"code int"
            f")"
        )

        cursor.execute(f"alter table {table_name} replica identity full")

        connection.commit()

        cursor.execute(
            "select slot_name "
            "from pg_replication_slots"
            f" where slot_name = '{POSTGRESQL_CDC_SLOT}'"
        )
        if not cursor.fetchone():
            cursor.execute(
                f"select pg_create_logical_replication_slot"
                f"("
                f"'{POSTGRESQL_CDC_SLOT}', "
                f"'wal2json'"
                f")"
            )
            cursor.fetchone()

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


def check_cdc_postgresql_replica_configuration(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = postgresql_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute("show wal_level")
        row = cursor.fetchone()
        if not row or row[0] != "logical":
            raise RuntimeError(
                f"CDC check failed: wal_level is not 'logical' (got {row})"
            )

        cursor.execute(
            "select slot_name "
            "from pg_replication_slots "
            f"where slot_name = '{POSTGRESQL_CDC_SLOT}'"
        )
        if not cursor.fetchone():
            raise RuntimeError(
                f"CDC check failed: replication slot '{POSTGRESQL_CDC_SLOT}' not found"
            )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_postgresql_replica_operation_slot(port: int, signal: str) -> None:
    connection = None
    cursor = None
    try:
        connection = postgresql_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(f"select relreplident from pg_class where relname = '{signal}'")
        row = cursor.fetchone()
        if not row or row[0] != "f":
            raise RuntimeError(
                f"CDC check failed: {signal} replica identity is not full (got {row})"
            )

        logger.info("PostgreSQL CDC setup verification passed")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_postgresql_replica_operation_stream(port: int, signal: str) -> None:
    expected_rows = [
        {"id": 1, "name": "Go Seigen", "code": 21},
        {"id": 2, "name": "Lee Sedol", "code": 17},
        {"id": 3, "name": "Kitani Minoru", "code": 19},
    ]

    connection = None
    cursor = None
    try:
        connection = postgresql_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(
            f"select data "
            f"from pg_logical_slot_peek_changes("
            f"'{POSTGRESQL_CDC_SLOT}', "
            f"null, "
            f"null, "
            f"'format-version', '2', "
            f"'write-in-chunks', '1'"
            f")"
        )
        rows = cursor.fetchall()

        messages = [orjson.loads(row[0]) for row in rows]

        begin_count = sum(1 for m in messages if m.get("action") == "B")
        commit_count = sum(1 for m in messages if m.get("action") == "C")
        inserts = [m for m in messages if m.get("action") == "I"]

        if begin_count < 1:
            raise RuntimeError(
                f"CDC check failed: expected at least 1 BEGIN message, "
                f"got {begin_count}"
            )

        if commit_count < 1:
            raise RuntimeError(
                f"CDC check failed: expected at least 1 COMMIT message, "
                f"got {commit_count}"
            )

        signal_inserts = [
            message for message in inserts if message.get("table") == signal
        ]

        if len(signal_inserts) != len(expected_rows):
            raise RuntimeError(
                f"CDC check failed: expected {len(expected_rows)} "
                f"INSERT messages for '{signal}', "
                f"got {len(signal_inserts)}"
            )

        for expected, message in zip(expected_rows, signal_inserts):
            columns = {col["name"]: col["value"] for col in message.get("columns", [])}
            if columns != expected:
                raise RuntimeError(
                    f"CDC check failed: expected row {expected}, got {columns}"
                )

        logger.info(
            f"PostgreSQL CDC wal2json stream verification passed "
            f"({len(signal_inserts)} rows with correct values "
            f"captured from WAL for '{signal}')"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def create_cdc_postgresql_container(
    name: str = DEFAULT_PYTEST_POSTGRESQL_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int]:
    container, port = ensure_container(
        name=name,
        image=POSTGRESQL_CDC_IMAGE,
        environment=[
            f"POSTGRES_DB={POSTGRESQL_CDC_DATABASE}",
            f"POSTGRES_USER={POSTGRESQL_CDC_ROOT_USER}",
            f"POSTGRES_PASSWORD={POSTGRESQL_CDC_ROOT_PASSWORD}",
        ],
        forwarded_ports={POSTGRESQL_CDC_FORWARDED_PORT: None},
        command=[
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
            "-c",
            "wal_keep_size=1024",
            "-c",
            "max_logical_replication_workers=4",
        ],
    )
    return container, port


def await_cdc_postgresql_container(port: int) -> None:
    def connect() -> None:
        connection = None
        cursor = None
        try:
            connection = postgresql_conn(port)
            cursor = connection.cursor()
            cursor.execute("select 1 from pg_catalog.pg_class limit 1")
            cursor.fetchone()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    ensure_connection(connect, "PostgreSQL CDC")


def setup_cdc_postgresql_container(container: Container, port: int) -> str:
    setup_cdc_postgresql_wal2json(container)
    setup_cdc_postgresql_user(port)
    signal = setup_cdc_postgresql_signal(port)

    return signal


def check_cdc_postgresql_container(port: int, signal: str) -> None:
    check_cdc_postgresql_replica_configuration(port)
    check_cdc_postgresql_replica_operation_slot(port, signal)
    check_cdc_postgresql_replica_operation_stream(port, signal)


def create_cdc_postgresql_database(
    name: str = DEFAULT_PYTEST_POSTGRESQL_CDC_DOCKER_CONTAINER_NAME,
) -> int:
    container, port = create_cdc_postgresql_container(name)
    await_cdc_postgresql_container(port)
    signal = setup_cdc_postgresql_container(container, port)
    check_cdc_postgresql_container(port, signal)

    return port


@pytest.fixture(scope="function")
def postgresql_cdc_config(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> PostgreSQLCDCConfig:
    prefix = obtain_prefix(
        request,
        POSTGRESQL_CDC_MARKER,
        POSTGRESQL_CDC_DEFAULT_PREFIX,
        VALID_POSTGRESQL_CDC_PREFIXES,
    )
    env_prefix = f"{prefix}__{POSTGRESQL_CDC_ENV_SUFFIX}"
    if os.environ.get(f"{env_prefix}_ENVS") in TRUE_VALUES:
        envs = obtain_environment(
            env_prefix,
            POSTGRESQL_CDC_REQUIRED_ENVS,
            "postgresql_cdc_config",
        )
        return PostgreSQLCDCConfig(
            host=envs["HOST"],
            port=int(envs["PORT"]),
            database=envs["DATABASE"],
            slot=envs["SLOT"],
            root_user=envs["ROOT_USER"],
            root_password=envs["ROOT_PASSWORD"],
            cdc_user=envs["CDC_USER"],
            cdc_password=envs["CDC_PASSWORD"],
        )
    else:
        name = os.environ.get(
            f"{env_prefix}_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_POSTGRESQL_CDC_DOCKER_CONTAINER_NAME,
        )
        port = -1
        if worker_id == PYTEST_XDIST_MASTER:
            port = create_cdc_postgresql_database(name)
        else:
            root_temp_dir = tmp_path_factory.getbasetemp().parent
            lock_file = root_temp_dir / POSTGRESQL_CDC_LOCK
            with FileLock(str(lock_file) + DOCKER_LOCK_SUFFIX):
                port = create_cdc_postgresql_database(name)
        return PostgreSQLCDCConfig(
            host=DB_HOST,
            port=int(port),
            database=POSTGRESQL_CDC_DATABASE,
            slot=POSTGRESQL_CDC_SLOT,
            root_user=POSTGRESQL_CDC_ROOT_USER,
            root_password=POSTGRESQL_CDC_ROOT_PASSWORD,
            cdc_user=POSTGRESQL_CDC_USER,
            cdc_password=POSTGRESQL_CDC_PASSWORD,
        )
