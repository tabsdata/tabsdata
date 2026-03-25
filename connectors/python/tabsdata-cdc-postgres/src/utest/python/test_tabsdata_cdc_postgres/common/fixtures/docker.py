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
from psycopg2.extras import LogicalReplicationConnection
import docker
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

DEFAULT_PYTEST_POSTGRES_CDC_DOCKER_CONTAINER_NAME: str = (
    f"{CDC_CONTAINER_PREFIX}postgres-cdc"
)

POSTGRES_CDC_FORWARDED_PORT: str = "5432/tcp"
POSTGRES_CDC_IMAGE: str = "postgres:17.2"

POSTGRES_CDC_DATABASE: str = CDC_DATABASE
POSTGRES_CDC_ROOT_USER: str = DB_USER
POSTGRES_CDC_ROOT_PASSWORD: str = DB_PASSWORD
POSTGRES_CDC_USER: str = CDC_USER
POSTGRES_CDC_PASSWORD: str = CDC_PASSWORD
POSTGRES_CDC_SLOT: str = "tdcdcdb_slot"

VALID_POSTGRES_CDC_PREFIXES: list[str] = ["POSTGRESCDC0"]
POSTGRES_CDC_DEFAULT_PREFIX: str = "POSTGRESCDC0"
POSTGRES_CDC_LOCK: str = "docker_cdc_postgres_creation"
POSTGRES_CDC_MARKER: str = "cdc_postgres"
POSTGRES_CDC_ENV_SUFFIX: str = "POSTGRESCDC"
POSTGRES_CDC_REQUIRED_ENVS: list[str] = [
    "HOST",
    "PORT",
    "DATABASE",
    "SLOT",
    "ROOT_USER",
    "ROOT_PASSWORD",
    "CDC_USER",
    "CDC_PASSWORD",
]

POSTGRES_CDC_SIGNAL_TABLE: str = "td_cdc_signal"


@dataclass
class PostgresCdcConfig:
    container: Container
    host: str
    port: int
    database: str
    slot: str
    root_user: str
    root_password: str
    cdc_user: str
    cdc_password: str


def postgres_conn(port: int) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST,
        port=str(port),
        dbname=POSTGRES_CDC_DATABASE,
        user=POSTGRES_CDC_ROOT_USER,
        password=POSTGRES_CDC_ROOT_PASSWORD,
    )


def postgres_exec(container: Container, command: str, user: str | None = None) -> str:
    code, output = container.exec_run(
        ["bash", "-c", command],
        **({"user": user} if user else {}),
    )
    output = output.decode("utf-8", errors="replace")
    if code != 0:
        raise RuntimeError(f"Postgres command failed:\n{command}\n\n{output}")
    return output


def setup_cdc_postgres_wal2json(container: Container) -> None:
    postgres_exec(
        container,
        "apt-get update && apt-get install -y postgresql-17-wal2json",
    )

    logger.info("Setting wal2json plugin completed successfully")

    postgres_exec(
        container,
        "pg_ctl reload -D /var/lib/postgresql/data",
        user="postgres",
    )

    logger.info("Reload of Postgres configuration completed successfully")


def setup_cdc_postgres_user(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = postgres_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(
            f"do $$ "
            f"begin "
            f"if not exists (select from pg_roles where rolname = "
            f"'{POSTGRES_CDC_USER}') then "
            f"create role {POSTGRES_CDC_USER} "
            f"with login password '{POSTGRES_CDC_PASSWORD}' "
            f"replication; "
            f"end if; "
            f"end $$"
        )

        cursor.execute(
            f"grant "
            f"connect on database {POSTGRES_CDC_DATABASE} "
            f"to {POSTGRES_CDC_USER}"
        )

        cursor.execute(f"grant usage on schema public to {POSTGRES_CDC_USER}")
        cursor.execute(
            f"grant select on all tables in schema public to {POSTGRES_CDC_USER}"
        )
        cursor.execute(
            f"alter default privileges in schema public "
            f"grant "
            f"select on tables "
            f"to {POSTGRES_CDC_USER}"
        )

        cursor.execute(
            f"create schema if not exists {POSTGRES_CDC_USER} "
            f"authorization {POSTGRES_CDC_USER}"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def setup_cdc_postgres_signal(port: int) -> str:
    table_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    table_name = f"{POSTGRES_CDC_SIGNAL_TABLE}_{table_suffix}"

    connection = None
    cursor = None
    try:
        connection = postgres_conn(port)
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
            f" where slot_name = '{POSTGRES_CDC_SLOT}'"
        )
        if not cursor.fetchone():
            cursor.execute(
                f"select pg_create_logical_replication_slot"
                f"("
                f"'{POSTGRES_CDC_SLOT}', "
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


def check_cdc_postgres_replica_configuration(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = postgres_conn(port)
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
            f"where slot_name = '{POSTGRES_CDC_SLOT}'"
        )
        if not cursor.fetchone():
            raise RuntimeError(
                f"CDC check failed: replication slot '{POSTGRES_CDC_SLOT}' not found"
            )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_postgres_replica_operation_slot(port: int, signal: str) -> None:
    connection = None
    cursor = None
    try:
        connection = postgres_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(f"select relreplident from pg_class where relname = '{signal}'")
        row = cursor.fetchone()
        if not row or row[0] != "f":
            raise RuntimeError(
                f"CDC check failed: {signal} replica identity is not full (got {row})"
            )

        logger.info("Postgres CDC setup verification passed")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def check_cdc_postgres_replica_operation_stream(port: int, signal: str) -> None:
    expected_rows = [
        {"id": 1, "name": "Go Seigen", "code": 21},
        {"id": 2, "name": "Lee Sedol", "code": 17},
        {"id": 3, "name": "Kitani Minoru", "code": 19},
    ]

    connection = None
    cursor = None
    try:
        connection = postgres_conn(port)
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(
            f"select data "
            f"from pg_logical_slot_peek_changes("
            f"'{POSTGRES_CDC_SLOT}', "
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
            f"Postgres CDC wal2json stream verification passed "
            f"({len(signal_inserts)} rows with correct values "
            f"captured from WAL for '{signal}')"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def create_cdc_postgres_container(
    name: str = DEFAULT_PYTEST_POSTGRES_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int, bool]:
    container, port, created = ensure_container(
        name=name,
        image=POSTGRES_CDC_IMAGE,
        environment=[
            f"POSTGRES_DB={POSTGRES_CDC_DATABASE}",
            f"POSTGRES_USER={POSTGRES_CDC_ROOT_USER}",
            f"POSTGRES_PASSWORD={POSTGRES_CDC_ROOT_PASSWORD}",
        ],
        forwarded_ports={POSTGRES_CDC_FORWARDED_PORT: None},
        command=[
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=1024",
            "-c",
            "max_wal_senders=1024",
            "-c",
            "wal_keep_size=1024",
            "-c",
            "max_logical_replication_workers=4",
        ],
    )
    return container, port, created


def await_cdc_postgres_container(port: int) -> None:
    def connect() -> None:
        connection = None
        cursor = None
        try:
            connection = postgres_conn(port)
            cursor = connection.cursor()
            cursor.execute("select 1 from pg_catalog.pg_class limit 1")
            cursor.fetchone()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    ensure_connection(connect, "Postgres CDC")


def setup_cdc_postgres_container(container: Container, port: int) -> str:
    setup_cdc_postgres_wal2json(container)
    setup_cdc_postgres_user(port)
    signal = setup_cdc_postgres_signal(port)

    return signal


def check_cdc_postgres_container(port: int, signal: str) -> None:
    check_cdc_postgres_replica_configuration(port)
    check_cdc_postgres_replica_operation_slot(port, signal)
    check_cdc_postgres_replica_operation_stream(port, signal)


def create_cdc_postgres_database(
    name: str = DEFAULT_PYTEST_POSTGRES_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int]:
    container, port, created = create_cdc_postgres_container(name)
    await_cdc_postgres_container(port)
    if created:
        signal = setup_cdc_postgres_container(container, port)
        check_cdc_postgres_container(port, signal)

    return container, port


@pytest.fixture(scope="function")
def postgres_cdc_config(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> PostgresCdcConfig:
    prefix = obtain_prefix(
        request,
        POSTGRES_CDC_MARKER,
        POSTGRES_CDC_DEFAULT_PREFIX,
        VALID_POSTGRES_CDC_PREFIXES,
    )
    env_prefix = f"{prefix}__{POSTGRES_CDC_ENV_SUFFIX}"
    if os.environ.get(f"{env_prefix}_ENVS") in TRUE_VALUES:
        envs = obtain_environment(
            env_prefix,
            POSTGRES_CDC_REQUIRED_ENVS,
            "postgres_cdc_config",
        )
        port = int(envs["PORT"])
        container = None
        client = docker.from_env()
        for c in client.containers.list():
            bindings = (
                c.attrs.get("NetworkSettings", {})
                .get("Ports", {})
                .get(POSTGRES_CDC_FORWARDED_PORT, [])
            ) or []
            for binding in bindings:
                if int(binding.get("HostPort", 0)) == port:
                    container = c
                    break
            if container is not None:
                break
        return PostgresCdcConfig(
            container=container,
            host=envs["HOST"],
            port=port,
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
            DEFAULT_PYTEST_POSTGRES_CDC_DOCKER_CONTAINER_NAME,
        )
        container = None
        port = -1
        if worker_id == PYTEST_XDIST_MASTER:
            container, port = create_cdc_postgres_database(name)
        else:
            root_temp_dir = tmp_path_factory.getbasetemp().parent
            lock_file = root_temp_dir / POSTGRES_CDC_LOCK
            with FileLock(str(lock_file) + DOCKER_LOCK_SUFFIX):
                container, port = create_cdc_postgres_database(name)
        return PostgresCdcConfig(
            container=container,
            host=DB_HOST,
            port=int(port),
            database=POSTGRES_CDC_DATABASE,
            slot=POSTGRES_CDC_SLOT,
            root_user=POSTGRES_CDC_ROOT_USER,
            root_password=POSTGRES_CDC_ROOT_PASSWORD,
            cdc_user=POSTGRES_CDC_USER,
            cdc_password=POSTGRES_CDC_PASSWORD,
        )


@pytest.fixture(scope="function")
def postgres_cdc_slot(postgres_cdc_config):
    config = postgres_cdc_config
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=32))
    slot_name = f"td_slot_{suffix}"
    rconn = psycopg2.connect(
        host=config.host,
        port=str(config.port),
        dbname=config.database,
        user=config.cdc_user,
        password=config.cdc_password,
        connection_factory=LogicalReplicationConnection,
    )
    rcursor = rconn.cursor()
    rcursor.create_replication_slot(slot_name, output_plugin="wal2json")
    rcursor.close()
    rconn.close()
    return slot_name
