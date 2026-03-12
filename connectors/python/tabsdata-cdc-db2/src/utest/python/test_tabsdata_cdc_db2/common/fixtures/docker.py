#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import random
import string
from dataclasses import dataclass

import ibm_db_dbi
import pytest
import docker
from docker.models.containers import Container
from filelock import FileLock
from tenacity import retry, stop_after_attempt, wait_incrementing

from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    CONTAINER_MAXIMUM_RETRY_COUNT,
    DB_HOST,
    DB_PASSWORD,
)
from test_tabsdata_cdc_common.common.docker import (
    CDC_CONTAINER_PREFIX,
    CDC_DATABASE,
    DOCKER_LOCK_SUFFIX,
    PYTEST_XDIST_MASTER,
    ensure_connection,
    ensure_container,
    obtain_environment,
    obtain_prefix,
)

logger = logging.getLogger(__name__)

DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME: str = f"{CDC_CONTAINER_PREFIX}db2-cdc"

DB2_CDC_FORWARDED_PORT: str = "50000/tcp"
DB2_CDC_IMAGE: str = "icr.io/db2_community/db2:latest"
DB2_CDC_DOCKER_DONE_MESSAGE: str = "Setup has completed."

DB2_CDC_DATABASE: str = CDC_DATABASE
DB2_CDC_ROOT_USER: str = "db2inst1"
DB2_CDC_ROOT_PASSWORD: str = DB_PASSWORD

VALID_DB2_CDC_PREFIXES: list[str] = ["DB2CDC0"]
DB2_CDC_DEFAULT_PREFIX: str = "DB2CDC0"
DB2_CDC_LOCK: str = "docker_cdc_db2_creation"
DB2_CDC_MARKER: str = "cdc_db2"
DB2_CDC_ENV_SUFFIX: str = "DB2CDC"
DB2_CDC_REQUIRED_ENVS: list[str] = [
    "HOST",
    "PORT",
    "DATABASE",
    "ROOT_USER",
    "ROOT_PASSWORD",
]

DB2_CDC_ARCHIVE_FOLDER: str = "archive"
DB2_CDC_BACKUP_FOLDER: str = "backup"
DB2_CDC_ASN_SCHEMA: str = "ASN"
DB2_CDC_STAGING_TABLE_PREFIX: str = "TD_T__"
DB2_CDC_OLD_VALUES_PREFIX: str = "_"
DB2_CDC_CONTROL_SCRIPT: str = "td_asn_control"
DB2_CDC_REGISTER_SCRIPT: str = "td_asn_register"

DB2_CDC_SIGNAL_TABLE: str = "td_cdc_signal"


@dataclass
class Db2CdcConfig:
    container: Container
    host: str
    port: int
    database: str
    root_user: str
    root_password: str
    cdc_user: str
    cdc_password: str

def db2_conn(port: int) -> ibm_db_dbi.Connection:
    return ibm_db_dbi.connect(
        host=DB_HOST,
        dsn=f"PORT={port};PROTOCOL=TCPIP;",
        database=DB2_CDC_DATABASE,
        user=DB2_CDC_ROOT_USER,
        password=DB2_CDC_ROOT_PASSWORD,
    )


def db2_exec(container: Container, command: str) -> str:
    code, output = container.exec_run(
        ["bash", "-l", "-c", command],
        user="db2inst1",
    )
    output = output.decode("utf-8", errors="replace")
    if code != 0:
        raise RuntimeError(f"Db2 command failed:\n{command}\n\n{output}")
    return output


def get_db2_home(container: Container) -> str:
    home = db2_exec(
        container,
        "printf '%s' \"$HOME\"",
    ).strip()
    if not home:
        raise RuntimeError("Unable to determine Db2 home folder inside container")

    logger.info(f"Getting Db2 home folder completed successfully: '{home}'")

    return home


def setup_db2_archive(container: Container) -> None:
    db2_exec(
        container,
        f"mkdir -p $HOME/{DB2_CDC_ARCHIVE_FOLDER} $HOME/{DB2_CDC_BACKUP_FOLDER}/",
    )
    db2_exec(
        container,
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 update db cfg for {DB2_CDC_DATABASE} "
        f"using logarchmeth1 disk:$HOME/{DB2_CDC_ARCHIVE_FOLDER}/ && "
        f"db2 terminate && "
        f"db2 backup db {DB2_CDC_DATABASE} to $HOME/{DB2_CDC_BACKUP_FOLDER}/",
    )

    logger.info("Setting Db2 archive log completed successfully")


def setup_cdc_db2_asn(container: Container) -> None:
    db2_exec(
        container,
        f'cd "$HOME" && '
        f"printf "
        f"'"
        f"set output capture script {DB2_CDC_CONTROL_SCRIPT}\\n"
        f"set server capture to db {DB2_CDC_DATABASE}\\n"
        f"set capture schema source {DB2_CDC_ASN_SCHEMA}\\n"
        f"create control tables for capture server\\n"
        f"quit\\n"
        f"' "
        f"| "
        f"asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {DB2_CDC_CONTROL_SCRIPT.upper()} && "
        f"db2 terminate",
    )

    logger.info("Setting asn system configuration completed successfully")


def setup_cdc_db2_asncap(container: Container, port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"update {DB2_CDC_ASN_SCHEMA}.ibmsnap_capparms "
            "set "
            "capture_path = ?, "
            "autostop = 'N', "
            "term = 'N'",
            (get_db2_home(container),),
        )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    logger.info("Configuration parameters for asncap updated")

    db2_exec(
        container,
        f"nohup asncap capture_server={DB2_CDC_DATABASE} "
        f"capture_schema={DB2_CDC_ASN_SCHEMA} "
        f"> /dev/null 2>&1 &",
    )
    logger.info("Bootstrap of asncap started")

    def wait_for_process(retries: int = CONTAINER_MAXIMUM_RETRY_COUNT) -> None:
        @retry(
            stop=stop_after_attempt(retries),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def check() -> None:
            logger.info("Checking if asncap is running and responsive...")
            db2_exec(
                container,
                f"asnccmd capture_schema={DB2_CDC_ASN_SCHEMA} "
                f"capture_server={DB2_CDC_DATABASE} status",
            )
            logger.info("Capture agent asncap is running and responsive")

        try:
            check()
        except Exception as exception:
            logger.error(
                f"Capture agent asncap did not become responsive "
                f"after {retries} attempts: {exception}"
            )
            raise

    wait_for_process()

    logger.info("Capture agent asncap is running")

    def wait_for_state(retries: int = CONTAINER_MAXIMUM_RETRY_COUNT) -> None:
        @retry(
            stop=stop_after_attempt(retries),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def check() -> None:
            logger.info("Checking if asncap has populated restart table...")
            i_connection = None
            i_cursor = None
            try:
                i_connection = db2_conn(port)
                i_cursor = i_connection.cursor()
                i_cursor.execute(
                    f"select max_commitseq from {DB2_CDC_ASN_SCHEMA}.ibmsnap_restart"
                )
                row = i_cursor.fetchone()
                if not row or row[0] is None:
                    raise RuntimeError("Restart table has no valid max_commitseq")
                logger.info("Restart table has valid max_commitseq")
            finally:
                if i_cursor is not None:
                    i_cursor.close()
                if i_connection is not None:
                    i_connection.close()

        try:
            check()
        except Exception as exception:
            logger.error(
                f"Restart table did not get populated "
                f"after {retries} attempts: {exception}"
            )
            raise

    wait_for_state()

    logger.info("Setting asncap process completed successfully")


def reinit_cdc_db2_asncap(container: Container) -> None:
    db2_exec(
        container,
        f"asnccmd capture_schema={DB2_CDC_ASN_SCHEMA} "
        f"capture_server={DB2_CDC_DATABASE} reinit",
    )

    logger.info("Capture agent reinitialization completed successfully")


def register_table_for_cdc(
    container: Container,
    port: int,
    schema: str,
    table: str,
    ddl: str,
    wait: bool = True,
) -> None:
    source_table = f"{schema.upper()}.{table.upper()}"
    staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}"
    register_script = f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}"
    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(ddl)
        cursor.execute(f"alter table {schema}.{table} data capture changes")
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    logger.info(f"Created table {source_table} with data capture enabled")

    db2_exec(
        container,
        f'cd "$HOME" && '
        f"printf "
        f"'"
        f"set output capture script {register_script}\\n"
        f"set server capture to db {DB2_CDC_DATABASE}\\n"
        f"set capture schema source {DB2_CDC_ASN_SCHEMA}\\n"
        f"create registration ({source_table}) "
        f"differential refresh "
        f"stage {DB2_CDC_ASN_SCHEMA}.{staging_table} "
        f"cols all "
        f"image both "
        f"prefix {DB2_CDC_OLD_VALUES_PREFIX} "
        f"options "
        f"capture all\\n"
        f"quit\\n"
        f"' "
        f"| asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {register_script.upper()} && "
        f"db2 terminate",
    )
    logger.info(f"Registration of {source_table} for data capture finished")

    # reinit_cdc_db2_asncap(container)

    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"select coalesce(max(cast(map_id as int)), 0) + 1 "
            f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_pruncntl"
        )
        map_id = str(cursor.fetchone()[0])
        cursor.execute(
            f"insert into {DB2_CDC_ASN_SCHEMA}.ibmsnap_pruncntl "
            f"("
            f"target_server, "
            f"target_owner, "
            f"target_table, "
            f"source_owner, "
            f"source_table, "
            f"source_view_qual, "
            f"apply_qual, "
            f"set_name, "
            f"cntl_server, "
            f"target_structure, "
            f"phys_change_owner, "
            f"phys_change_table, "
            f"map_id"
            f") "
            f"values ("
            f"'{DB2_CDC_DATABASE}', "
            f"'{DB2_CDC_ASN_SCHEMA}', "
            f"'{staging_table}', "
            f"?, "
            f"?, "
            f"0, "
            f"'TDQUAL', "
            f"'TDSET', "
            f"'{DB2_CDC_DATABASE}', "
            f"1, "
            f"'{DB2_CDC_ASN_SCHEMA}', "
            f"'{staging_table}', "
            f"?"
            f")",
            (schema.upper(), table.upper(), map_id),
        )
        cursor.execute(
            f"insert into {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
            f"("
            f"signal_type, "
            f"signal_subtype, "
            f"signal_input_in, "
            f"signal_state"
            f") "
            f"values "
            f"("
            f"'CMD', "
            f"'CAPSTART', "
            f"?, "
            f"'P'"
            f")",
            (map_id,),
        )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    logger.info(f"CAPSTART signal inserted for {source_table} (map_id={map_id})")

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def wait_for_capstart() -> None:
        w_connection = None
        w_cursor = None
        try:
            w_connection = db2_conn(port)
            w_cursor = w_connection.cursor()
            w_cursor.execute(
                f"select signal_state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_subtype = 'CAPSTART' "
                f"and signal_input_in = ? "
                f"order by signal_time desc "
                f"fetch first 1 row only",
                (map_id,),
            )
            row = w_cursor.fetchone()
            if not row:
                raise RuntimeError("CAPSTART signal not found")
            state = row[0].strip()
            if state != "C":
                raise RuntimeError(
                    f"CAPSTART signal not yet processed (state='{state}')"
                )
        finally:
            if w_cursor is not None:
                w_cursor.close()
            if w_connection is not None:
                w_connection.close()

    if wait:
        wait_for_capstart()
        logger.info(f"CAPSTART signal for {source_table} processed by asncap")

    logger.info(
        f"Registration of {source_table} for data capture completed successfully"
    )


def setup_cdc_db2_signal(container: Container, port: int) -> str:
    table_suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    table_name = f"{DB2_CDC_SIGNAL_TABLE}_{table_suffix}"

    register_table_for_cdc(
        container,
        port,
        schema=DB2_CDC_ROOT_USER,
        table=table_name,
        ddl=(
            f"create table {table_name} "
            f"("
            f"id int generated by default as identity, "
            f"name varchar(255), "
            f"code int"
            f")"
        ),
    )
    logger.info(f"Creation of signal table '{table_name}' completed")

    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
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
    logger.info(f"Insertion of data in the cdc signal table '{table_name}' completed")

    logger.info(f"Setup of the signal table '{table_name}' completed successfully")

    return table_name


def check_cdc_db2_log(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute("select value from sysibmadm.dbcfg where name = 'logarchmeth1'")
        row = cursor.fetchone()
        if not row or not row[0] or not row[0].strip().upper().startswith("DISK:"):
            raise RuntimeError(
                f"Db2 CDC check failed: logarchmeth1 is not set to DISK (got {row})"
            )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info("Db2 archive configuration is fine")


def check_cdc_db2_asn_registration(port: int, signal: str) -> None:
    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check() -> None:
        connection = None
        cursor = None
        try:
            connection = db2_conn(port)
            cursor = connection.cursor()
            cursor.execute(
                "select "
                "source_owner, "
                "source_table, "
                "cd_owner, "
                "cd_table, "
                "state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                f"where source_table = '{signal.upper()}'"
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    f"Db2 CDC check failed: "
                    f"{signal} not registered in"
                    f" {DB2_CDC_ASN_SCHEMA}.ibmsnap_register"
                )
            registration_state = row[4].strip()
            if registration_state.upper() != "A":
                raise RuntimeError(
                    f"Db2 CDC check failed: registration for "
                    f"table '{signal}' is not active "
                    f"(state='{registration_state}')"
                )
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    try:
        logger.info(f"Checking registration for signal table '{signal}'...")
        check()
        logger.info(f"Registration for signal table '{signal}' is active")
    except Exception as exception:
        logger.error(
            f"Registration for signal table '{signal}' did not become active "
            f"after {CONTAINER_MAXIMUM_RETRY_COUNT} attempts: {exception}"
        )
        raise


def check_cdc_db2_asn_operation(port: int, signal: str) -> None:
    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()

        cursor.execute(f"select count(*) from {signal}")
        count = cursor.fetchone()[0]
        if count == 0:
            raise RuntimeError(
                f"Db2 CDC check failed: signal table '{signal}' has no data"
            )

        cursor.execute(
            "select "
            "cd_owner, "
            "cd_table "
            f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
            f"where source_table = '{signal.upper()}'"
        )
        row = cursor.fetchone()
        signal_cd_owner = row[0].strip()
        signal_cd_table = row[1].strip()
        expected_cd_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{signal.upper()}"
        if signal_cd_table != expected_cd_table:
            raise RuntimeError(
                f"Db2 CDC check failed: "
                f"staging table is '{signal_cd_table}'"
                f" but expected '{expected_cd_table}'"
            )

        @retry(
            stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def wait_for_captured_data() -> None:
            cursor.execute(f"select count(*) from {signal_cd_owner}.{signal_cd_table}")
            cd_count = cursor.fetchone()[0]
            logger.info(
                f"Staging table {signal_cd_owner}.{signal_cd_table} "
                f"has {cd_count} captured rows"
            )
            if cd_count == 0:
                raise RuntimeError(
                    f"Db2 CDC check failed: "
                    f"staging table {signal_cd_owner}.{signal_cd_table} "
                    f"has no captured data"
                )

        wait_for_captured_data()

        cursor.execute(
            f"select colname "
            f"from syscat.columns "
            f"where tabschema = '{signal_cd_owner}' "
            f"and tabname = '{signal_cd_table}' "
            "order by colno"
        )
        cd_columns = [row[0].strip() for row in cursor.fetchall()]
        if not cd_columns:
            raise RuntimeError(
                f"Db2 CDC check failed: "
                f"no columns found for {signal_cd_owner}.{signal_cd_table}"
            )
        for expected in ["ID", "NAME", "CODE"]:
            if expected not in cd_columns:
                raise RuntimeError(
                    f"Db2 CDC check failed: "
                    f"source column '{expected}' not found"
                    f" in staging table {signal_cd_owner}.{signal_cd_table}"
                    f" (columns: {cd_columns})"
                )

        for expected in ["ID", "NAME", "CODE"]:
            old_col = f"{DB2_CDC_OLD_VALUES_PREFIX}{expected}"
            if old_col not in cd_columns:
                raise RuntimeError(
                    f"Db2 CDC check failed: "
                    f"old-value column '{old_col}' not found"
                    f" in staging table {signal_cd_owner}.{signal_cd_table}"
                    f" (columns: {cd_columns})"
                )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info(
        f"Db2 CDC asn operation verification for signal table '{signal}' passed"
    )


def create_cdc_db2_container(
    name: str = DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int, bool]:
    container, port, created = ensure_container(
        name=name,
        image=DB2_CDC_IMAGE,
        environment=[
            f"DB2INST1_PASSWORD={DB2_CDC_ROOT_PASSWORD}",
            f"DBNAME={DB2_CDC_DATABASE}",
            "LICENSE=accept",
        ],
        forwarded_ports={DB2_CDC_FORWARDED_PORT: None},
        privileged=True,
        platform="linux/amd64",
        message=DB2_CDC_DOCKER_DONE_MESSAGE,
    )
    return container, port, created


def await_cdc_db2_container(port: int) -> None:

    def connect() -> None:
        connection = None
        cursor = None
        try:
            connection = db2_conn(port)
            cursor = connection.cursor()
            cursor.execute("select 1 from sysibm.sysdummy1")
            cursor.fetchone()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    ensure_connection(connect, "Db2 CDC")


def setup_cdc_db2_container(container: Container, port: int) -> str:
    get_db2_home(container)
    setup_db2_archive(container)
    setup_cdc_db2_asn(container)
    setup_cdc_db2_asncap(container, port)
    signal = setup_cdc_db2_signal(container, port)

    return signal


def check_cdc_db2_container(port: int, signal: str) -> None:
    check_cdc_db2_log(port)
    check_cdc_db2_asn_registration(port, signal)
    check_cdc_db2_asn_operation(port, signal)


def create_cdc_db2_database(
    name: str = DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME,
) -> tuple[Container, int]:
    container, port, created = create_cdc_db2_container(name)
    await_cdc_db2_container(port)
    if created:
        signal = setup_cdc_db2_container(container, port)
        check_cdc_db2_container(port, signal)

    return container, port


@pytest.fixture(scope="function")
def db2_cdc_config(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> Db2CdcConfig:
    prefix = obtain_prefix(
        request,
        DB2_CDC_MARKER,
        DB2_CDC_DEFAULT_PREFIX,
        VALID_DB2_CDC_PREFIXES,
    )
    env_prefix = f"{prefix}__{DB2_CDC_ENV_SUFFIX}"
    if os.environ.get(f"{env_prefix}_ENVS") in TRUE_VALUES:
        envs = obtain_environment(
            env_prefix,
            DB2_CDC_REQUIRED_ENVS,
            "db2_cdc_config",
        )
        port = int(envs["PORT"])
        container = None
        client = docker.from_env()
        for c in client.containers.list():
            bindings = (
                c.attrs.get("NetworkSettings", {})
                .get("Ports", {})
                .get(DB2_CDC_FORWARDED_PORT, [])
            ) or []
            for binding in bindings:
                if int(binding.get("HostPort", 0)) == port:
                    container = c
                    break
            if container is not None:
                break
        return Db2CdcConfig(
            container=container,
            host=envs["HOST"],
            port=port,
            database=envs["DATABASE"],
            root_user=envs["ROOT_USER"],
            root_password=envs["ROOT_PASSWORD"],
        )
    else:
        name = os.environ.get(
            f"{env_prefix}_DOCKER_CONTAINER_NAME",
            DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME,
        )
        container = None
        port = -1
        if worker_id == PYTEST_XDIST_MASTER:
            container, port = create_cdc_db2_database(name)
        else:
            root_temp_dir = tmp_path_factory.getbasetemp().parent
            lock_file = root_temp_dir / DB2_CDC_LOCK
            with FileLock(str(lock_file) + DOCKER_LOCK_SUFFIX):
                container, port = create_cdc_db2_database(name)
        return Db2CdcConfig(
            container=container,
            host=DB_HOST,
            port=int(port),
            database=DB2_CDC_DATABASE,
            root_user=DB2_CDC_ROOT_USER,
            root_password=DB2_CDC_ROOT_PASSWORD,
            cdc_user=DB2_CDC_ROOT_USER,
            cdc_password=DB2_CDC_ROOT_PASSWORD,
        )
