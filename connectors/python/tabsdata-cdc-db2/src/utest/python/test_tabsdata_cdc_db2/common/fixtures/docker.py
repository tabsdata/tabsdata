#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import random
import string
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto

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


class CdcMode(Enum):
    SOFT = auto()
    HARD = auto()

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

ASNCAP_LOCK_PATH = os.path.join(tempfile.gettempdir(), "td_asncap_restart.lock")
ASNCAP_STATUS_WAIT_SECONDS = 30
ASNCAP_WATCHDOG_INTERVAL_SECONDS = 15
ASNCAP_STALE_SIGNAL_SECONDS = 120
ASNCAP_STOP_TIMEOUT_SECONDS = 60
ASNCAP_STOP_POLL_SECONDS = 1

asncap_lock = FileLock(ASNCAP_LOCK_PATH, timeout=60 * 30)


class AsncapWatchdog:
    _instance_lock = threading.Lock()
    _instance: AsncapWatchdog | None = None

    def __init__(self, container: Container, port: int):
        self._container = container
        self._port = port
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="asncap-watchdog",
            daemon=True,
        )

    def _run(self) -> None:
        logger.info("Watchdog for asncap started")
        while not self._stop_event.wait(ASNCAP_WATCHDOG_INTERVAL_SECONDS):
            try:
                with asncap_lock:
                    if self._stop_event.is_set():
                        break
                    if not is_asncap_responsive(self._container):
                        logger.warning(
                            "The asncap watchdog detected asncap is down; "
                            "restarting and reinitializing..."
                        )
                        restart_asncap(self._container, self._port)
                        reinit_asncap(self._container)
                    elif has_stale_pending_signals(self._port):
                        logger.warning(
                            "The asncap watchdog detected zombie asncap "
                            "(responding but not processing signals); "
                            "restarting and reinitializing..."
                        )
                        restart_asncap(self._container, self._port)
                        reinit_asncap(self._container)
            except Exception as exc:
                logger.warning(f"The asncap watchdog health check failed: {exc}")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        logger.info("Watchdog for asncap stopping")
        self._stop_event.set()
        self._thread.join(timeout=60)

    @classmethod
    def ensure_started(cls, container: Container, port: int) -> AsncapWatchdog:
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = AsncapWatchdog(container, port)
                cls._instance.start()
            return cls._instance

    @classmethod
    def shutdown(cls) -> None:
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance.stop()
                cls._instance = None


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


@contextmanager
def db2_cursor(port: int):
    connection = db2_conn(port)
    cursor = connection.cursor()
    try:
        yield cursor, connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


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
    control_script = DB2_CDC_CONTROL_SCRIPT.upper()
    db2_exec(
        container,
        f'cd "$HOME" && '
        f"rm -f {control_script} && "
        f"printf "
        f"'"
        f"set output capture script {control_script}\\n"
        f"set server capture to db {DB2_CDC_DATABASE}\\n"
        f"set capture schema source {DB2_CDC_ASN_SCHEMA}\\n"
        f"create control tables for capture server\\n"
        f"quit\\n"
        f"' "
        f"| "
        f"asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {control_script} && "
        f"db2 terminate",
    )

    logger.info("Setting asn system configuration completed successfully")


def setup_cdc_db2_asncap(container: Container, port: int) -> None:
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"update {DB2_CDC_ASN_SCHEMA}.ibmsnap_capparms "
            "set "
            "capture_path = ?, "
            "autostop = 'N', "
            "term = 'N'",
            (get_db2_home(container),),
        )
    logger.info("Configuration parameters for asncap updated")

    q_asncap_inits = count_asncap_inits(port)
    start_asncap(container)
    wait_asncap_responsive(container)
    wait_asncap_initialized(port, q_asncap_inits)

    def wait_for_state(retries: int = CONTAINER_MAXIMUM_RETRY_COUNT) -> None:
        @retry(
            stop=stop_after_attempt(retries),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def check() -> None:
            logger.info("Checking if asncap has populated restart table...")
            with db2_cursor(port) as (i_cursor, _):
                i_cursor.execute(
                    f"select max_commitseq "
                    f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_restart"
                )
                row = i_cursor.fetchone()
                if not row or row[0] is None:
                    raise RuntimeError("Restart table has no valid max_commitseq")
                logger.info("Restart table has valid max_commitseq")

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


def is_asncap_responsive(container: Container) -> bool:
    try:
        db2_exec(
            container,
            f"asnccmd capture_schema={DB2_CDC_ASN_SCHEMA} "
            f"capture_server={DB2_CDC_DATABASE} status",
        )
        return True
    except RuntimeError:
        return False


def is_asncap_alive(container: Container) -> bool:
    code, _ = container.exec_run(
        ["bash", "-l", "-c", "pgrep -f asncap"],
        user="db2inst1",
    )
    return code == 0


def send_capstart_signal(
    port: int,
    schema: str,
    table: str,
    staging_table: str,
) -> str:
    @retry(
        stop=stop_after_attempt(8),
        wait=wait_incrementing(start=0, increment=1),
        reraise=True,
    )
    def insert_pruncntl_and_signal():
        i_map_id = str(random.randint(1, 2**31 - 1))
        with db2_cursor(port) as (cursor, _):
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
                f"(signal_type, signal_subtype, signal_input_in, signal_state) "
                f"values ('CMD', 'CAPSTART', ?, 'P')",
                (map_id,),
            )
        return i_map_id

    map_id = insert_pruncntl_and_signal()
    source_table = f"{schema.upper()}.{table.upper()}"
    logger.info(f"CAPSTART signal inserted for {source_table} (map_id={map_id})")
    return map_id


def wait_capstart_consumed(port: int, map_id: str) -> None:
    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select signal_state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' "
                f"and signal_subtype = 'CAPSTART' "
                f"and signal_input_in = ? "
                f"order by signal_time desc "
                f"fetch first 1 row only",
                (map_id,),
            )
            row = cursor.fetchone()
            if not row:
                return
            state = row[0].strip()
            if state == "C":
                return
            raise RuntimeError(
                f"CAPSTART signal not yet processed (state='{state}')"
            )

    check()
    logger.info(f"CAPSTART signal consumed (map_id={map_id})")


def wait_registration_active(port: int, schema: str, table: str) -> None:
    source_table = f"{schema.upper()}.{table.upper()}"

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                f"where source_owner = ? "
                f"and source_table = ?",
                (schema.upper(), table.upper()),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    f"Registration for {source_table} not found in "
                    f"{DB2_CDC_ASN_SCHEMA}.ibmsnap_register"
                )
            state = row[0].strip()
            if state.upper() != "A":
                raise RuntimeError(
                    f"Registration for {source_table} is not active "
                    f"(state='{state}')"
                )

    check()
    logger.info(f"Registration for {source_table} is active")


def send_capstop_signal(port: int, schema: str, table: str) -> None:
    source_table = f"{schema.upper()}.{table.upper()}"
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"insert into {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
            f"(signal_type, signal_subtype, signal_input_in, signal_state) "
            f"values ('CMD', 'CAPSTOP', ?, 'P')",
            (source_table,),
        )
    logger.info(f"CAPSTOP signal inserted for {source_table}")


def wait_capstop_consumed(port: int, schema: str, table: str) -> None:
    source_table = f"{schema.upper()}.{table.upper()}"

    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select signal_state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' "
                f"and signal_subtype = 'CAPSTOP' "
                f"and signal_input_in = ? "
                f"order by signal_time desc "
                f"fetch first 1 row only",
                (source_table,),
            )
            row = cursor.fetchone()
            if not row:
                return
            state = row[0].strip()
            if state == "C":
                return
            raise RuntimeError(
                f"CAPSTOP signal not yet processed (state='{state}')"
            )

    check()
    logger.info(f"CAPSTOP signal consumed for {source_table}")


def has_stale_pending_signals(port: int) -> bool:
    try:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select count(*) "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' "
                f"and signal_subtype = 'CAPSTART' "
                f"and signal_state = 'P' "
                f"and signal_time < current timestamp - "
                f"{ASNCAP_STALE_SIGNAL_SECONDS} seconds"
            )
            row = cursor.fetchone()
            count = row[0] if row else 0
            if count > 0:
                logger.warning(
                    f"Found {count} stale pending CAPSTART signal(s) "
                    f"older than {ASNCAP_STALE_SIGNAL_SECONDS}s"
                )
            return count > 0
    except Exception as exception:
        logger.warning(f"Failed to check for stale pending signals: {exception}")
        return False


def start_asncap(container: Container) -> None:
    db2_exec(
        container,
        f"nohup asncap capture_server={DB2_CDC_DATABASE} "
        f"capture_schema={DB2_CDC_ASN_SCHEMA} "
        f"> /dev/null 2>&1 &",
    )
    logger.info("Start process asncap command issued")


def wait_asncap_responsive(container: Container) -> None:
    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check() -> None:
        if not is_asncap_responsive(container):
            raise RuntimeError("The asncap process is not yet responsive")

    check()
    logger.info("The asncap process is responsive")


def count_asncap_inits(port: int) -> int:
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"select count(*) "
            f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_captrace "
            f"where description like '%ASN0109I%'"
        )
        row = cursor.fetchone()
        return row[0] if row else 0


def wait_asncap_initialized(port: int, previous_q_asncap_inits: int) -> None:
    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def check_trace() -> None:
        current_q_asncap_inits = count_asncap_inits(port)
        if current_q_asncap_inits <= previous_q_asncap_inits:
            raise RuntimeError(
                f"Capture initialization trace (ASN0109I) not yet written "
                f"(previous={previous_q_asncap_inits}, current={current_q_asncap_inits})"
            )

    check_trace()
    logger.info("The asncap process initialization completed (ASN0109I)")


def wait_asncap_dead(container: Container) -> bool:
    for _ in range(ASNCAP_STOP_TIMEOUT_SECONDS):
        time.sleep(ASNCAP_STOP_POLL_SECONDS)
        if not is_asncap_alive(container):
            return True
    return False


def stop_asncap(container: Container) -> None:
    try:
        db2_exec(
            container,
            f"asnccmd capture_schema={DB2_CDC_ASN_SCHEMA} "
            f"capture_server={DB2_CDC_DATABASE} stop",
        )
        logger.info("Sent stop command to asncap")
    except RuntimeError:
        logger.warning("Failed to send stop command to asncap")
    if wait_asncap_dead(container):
        logger.info("Process asncap stopped gracefully")
        return

    container.exec_run(
        ["bash", "-l", "-c", "pkill -f asncap"],
        user="db2inst1",
    )
    logger.warning("Sent SIGTERM to asncap")
    if wait_asncap_dead(container):
        logger.info("Process asncap terminated after SIGTERM")
        return

    container.exec_run(
        ["bash", "-l", "-c", "pkill -9 -f asncap"],
        user="db2inst1",
    )
    logger.warning("Sent SIGKILL to asncap")
    time.sleep(5)
    if not is_asncap_alive(container):
        logger.info("Process asncap killed after SIGKILL")
    else:
        logger.error("Process asncap still alive after SIGKILL")


def reinit_asncap(container: Container) -> None:
    @retry(
        stop=stop_after_attempt(CONTAINER_MAXIMUM_RETRY_COUNT),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def attempt() -> None:
        logger.debug("Attempting asncap reinit...")
        db2_exec(
            container,
            f"asnccmd capture_schema={DB2_CDC_ASN_SCHEMA} "
            f"capture_server={DB2_CDC_DATABASE} reinit",
        )

    attempt()
    logger.info("Process asncap reinitialization completed successfully")


def ensure_asncap_running(container: Container) -> None:
    if is_asncap_responsive(container):
        return

    if is_asncap_alive(container):
        logger.info(
            "The asncap process exists but is not responsive; "
            f"waiting up to {ASNCAP_STATUS_WAIT_SECONDS}s for it to recover..."
        )

        @retry(
            stop=stop_after_attempt(ASNCAP_STATUS_WAIT_SECONDS),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def wait_for_recovery() -> None:
            if not is_asncap_responsive(container):
                raise RuntimeError("The asncap process is still not responsive")

        try:
            wait_for_recovery()
            logger.info("The asncap process became responsive")
            return
        except RuntimeError:
            logger.warning(
                "The asncap process exists but never became responsive; "
                "will attempt restart"
            )

    logger.warning("The asncap process is down; restarting...")
    start_asncap(container)
    wait_asncap_responsive(container)


def restart_asncap(container: Container, port: int) -> None:
    q_asncap_inits = count_asncap_inits(port)
    stop_asncap(container)
    start_asncap(container)
    wait_asncap_responsive(container)
    wait_asncap_initialized(port, q_asncap_inits)


def reinit_cdc_db2_asncap(container: Container) -> None:
    with asncap_lock:
        ensure_asncap_running(container)
        reinit_asncap(container)


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
    register_script = f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}".upper()
    with db2_cursor(port) as (cursor, _):
        cursor.execute(ddl)
        cursor.execute(f"alter table {schema}.{table} data capture changes")
    logger.info(f"Created table {source_table} with data capture enabled")

    db2_exec(
        container,
        f'cd "$HOME" && '
        f"rm -f {register_script} && "        
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
        f"db2 -tvf {register_script} && "
        f"db2 terminate",
    )
    logger.info(f"Registration of {source_table} for data capture finished")

    map_id = send_capstart_signal(port, schema, table, staging_table)

    if wait:
        wait_capstart_consumed(port, map_id)
        wait_registration_active(port, schema, table)

    logger.info(
        f"Registration of {source_table} for data capture completed successfully"
    )


def deregister_table_from_cdc(
    container: Container,
    port: int,
    schema: str,
    table: str,
    mode: CdcMode = CdcMode.HARD,
) -> None:
    source_table = f"{schema.upper()}.{table.upper()}"
    staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}"

    match mode:
        case CdcMode.SOFT:
            send_capstop_signal(port, schema, table)
            wait_capstop_consumed(port, schema, table)
        case CdcMode.HARD:
            pass
        case _:
            raise ValueError(f"Unknown CDC mode: {mode}")

    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"alter table {schema}.{table} data capture none"
        )
        connection.commit()
        match mode:
            case CdcMode.HARD:
                cursor.execute(
                    f"delete from {DB2_CDC_ASN_SCHEMA}.ibmsnap_pruncntl "
                    f"where source_owner = ? "
                    f"and source_table = ?",
                    (schema.upper(), table.upper()),
                )
                cursor.execute(
                    f"delete from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                    f"where source_owner = ? "
                    f"and source_table = ?",
                    (schema.upper(), table.upper()),
                )
                connection.commit()
                # noinspection PyBroadException
                try:
                    cursor.execute(
                        f"drop table {DB2_CDC_ASN_SCHEMA}.{staging_table}"
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
            case CdcMode.SOFT:
                cursor.execute(
                    f"delete from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                    f"where source_owner = ? "
                    f"and source_table = ?",
                    (schema.upper(), table.upper()),
                )
                connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    if mode == CdcMode.HARD:
        reinit_cdc_db2_asncap(container)

    logger.info(
        f"Deregistration ({mode.name}) of {source_table} from data capture completed"
    )


def reregister_table_for_cdc(
    container: Container,
    port: int,
    schema: str,
    table: str,
    wait: bool = True,
    mode: CdcMode = CdcMode.HARD,
    version: int = 0,
) -> str | None:
    source_table = f"{schema.upper()}.{table.upper()}"
    match mode:
        case CdcMode.HARD:
            staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}"
            register_script = f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}_rereg".upper()
        case CdcMode.SOFT:
            staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}_V{version}"
            register_script = f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}_v{version}".upper()
        case _:
            raise ValueError(f"Unknown CDC mode: {mode}")

    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            f"alter table {schema}.{table} data capture changes"
        )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info(f"Re-enabled data capture for {source_table}")

    db2_exec(
        container,
        f'cd "$HOME" && '
        f"rm -f {register_script} && "
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
        f"db2 -tvf {register_script} && "
        f"db2 terminate",
    )
    logger.info(f"Re-registration of {source_table} for data capture finished")

    map_id = send_capstart_signal(port, schema, table, staging_table)

    if wait:
        wait_capstart_consumed(port, map_id)
        wait_registration_active(port, schema, table)

    logger.info(
        f"Re-registration ({mode.name}) of {source_table} for data capture completed "
        f"(staging={staging_table})"
    )

    if mode == CdcMode.SOFT:
        return f"{DB2_CDC_ASN_SCHEMA.lower()}.{staging_table}"
    return None


def setup_cdc_db2_signal(container: Container, port: int) -> str:
    table_suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    table_name = f"{DB2_CDC_SIGNAL_TABLE}_{table_suffix}"

    register_table_for_cdc(
        container=container,
        port=port,
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
        wait=True,
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
                    f"{signal} not registered in {DB2_CDC_ASN_SCHEMA}.ibmsnap_register"
                )
            registration_state = row[4].strip()
            if registration_state.upper() != "A":
                raise RuntimeError(
                    f"Db2 CDC check failed: "
                    f"registration for table '{signal}' is not active "
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
        if container is not None:
            AsncapWatchdog.ensure_started(container, port)
        return Db2CdcConfig(
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
        AsncapWatchdog.ensure_started(container, port)
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
