#
# Copyright 2026 Tabs Data Inc.
#

"""Real-``asncap`` CDC fixture machinery for ``tabsdata-conn-db2``.

Port of the legacy ``test_tabsdata_cdc_db2.common.fixtures.docker``
module adapted to ride on top of the connector's own session-scoped
``db2_config`` container (``TESTDB``) instead of standing up a
separate ``tdcdcdb`` instance.

Surface
-------

* :func:`setup_db2_archive` -- archive logging + initial offline
  backup (mandatory before asncap can run).
* :func:`setup_cdc_db2_asn` -- ``asnclp create control tables for
  capture server`` to materialise the ``ASN.IBMSNAP_*`` registry.
* :func:`setup_cdc_db2_asncap` -- configure ``IBMSNAP_CAPPARMS`` +
  start ``asncap`` + wait for it to populate ``IBMSNAP_RESTART``.
* :func:`register_table_for_cdc` /
  :func:`deregister_table_from_cdc` /
  :func:`reregister_table_for_cdc` -- per-table lifecycle via
  ``asnclp create registration ... differential refresh`` and
  ``IBMSNAP_PRUNCNTL`` / ``IBMSNAP_SIGNAL``.
* :class:`AsncapWatchdog` -- background thread that restarts +
  reinits ``asncap`` when it goes down or starts ignoring signals.
* :func:`setup_cdc_db2_signal` -- registers a randomly-named
  signal table whose presence in ``IBMSNAP_REGISTER`` confirms
  the whole pipeline is wired end-to-end.
* :func:`setup_cdc_db2_container` / :func:`check_cdc_db2_container`
  -- one-shot composite that runs the archive + asn + asncap +
  signal-table sequence and verifies it against
  :func:`check_cdc_db2_log`, :func:`check_cdc_db2_asn_registration`
  and :func:`check_cdc_db2_asn_operation`.

The full surface is preserved so future tests can deregister /
re-register / soft-stop / hard-stop CDC the way the legacy suite
does, even though the initial ``test__cdc_plugin_integration2.py``
file only exercises the happy-path subset.
"""

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
from typing import Any, Callable

import docker
import ibm_db_dbi
import pytest
from docker.models.containers import Container
from filelock import FileLock
from tenacity import (
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
    wait_incrementing,
)

from tabsdata._utils.constants import TRUE_VALUES
from test_tabsdata.common.constants import (
    CONTAINER_MAXIMUM_RETRY_COUNT,
    DB_HOST,
    DB_PASSWORD,
    DOCKER_REGULAR_CONTAINER_INFIX,
    DOCKER_UTEST_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import (
    get_container_port_binding,
    pull_docker_image,
    wait_for_container_running,
)

logger = logging.getLogger(__name__)


# --- Timing -----------------------------------------------------------
#
# DB2 on emulation (linux/amd64 on ARM) can take 13+ minutes to
# initialise; the asncap-related polls inherit the same generous
# budget.
DB2_WAIT_TIMEOUT_SECONDS = 900  # 15 minutes
DB2_WAIT_POLL_INTERVAL_SECONDS = 5


# --- Inlined CDC-common helpers + constants --------------------------
#
# The legacy module pulled these from
# ``test_tabsdata_cdc_common.common.docker``; inlined here so this
# port has no cross-package dependency on the legacy CDC packages.

PYTEST_XDIST_MASTER: str = "master"
DOCKER_LOCK_SUFFIX: str = ".lock"
ENVIRONMENT_PREFIX: str = "env_prefix"
CDC_CONTAINER_PREFIX: str = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}{DOCKER_REGULAR_CONTAINER_INFIX}"
)


def ensure_container(
    name: str,
    image: str,
    environment: list[str],
    forwarded_ports: dict[str, None],
    command: list[str] | None = None,
    entrypoint: str | None = None,
    platform: str | None = None,
    privileged: bool = False,
    remove: bool = False,
    message: str | None = None,
) -> tuple[Container, int, bool]:
    """Reuse-or-create a container, returning ``(container, host_port, created)``.
    Mirrors the legacy CDC-common helper of the same name."""
    client = docker.from_env()
    forwarded_port_key = next(iter(forwarded_ports))

    if containers := client.containers.list(all=True, filters={"name": f"^{name}$"}):
        logger.info(f"CDC container '{name}' already exists; reusing it.")
        container = containers[0]
        wait_for_container_running(container)
        if message is not None:
            ensure_readiness(container, message)
        host_port = get_container_port_binding(container, forwarded_port_key)
        return container, host_port, False

    logger.info(f"Creating CDC container '{name}' from image '{image}'")
    run_kwargs: dict[str, Any] = {
        "name": name,
        "environment": environment,
        "ports": forwarded_ports,
        "detach": True,
        "privileged": privileged,
        "remove": remove,
    }
    if command is not None:
        run_kwargs["command"] = command
    if entrypoint is not None:
        run_kwargs["entrypoint"] = entrypoint
    if platform is not None:
        run_kwargs["platform"] = platform
    pull_docker_image(image, platform=platform)
    try:
        container = client.containers.run(image, **run_kwargs)
    except Exception as e:
        logger.error(f"Failed to create CDC container '{name}': {e}")
        raise
    container.reload()
    if message is not None:
        ensure_readiness(container, message)
    host_port = get_container_port_binding(container, forwarded_port_key)
    return container, host_port, True


def ensure_connection(
    function: Callable[[], None],
    database: str,
    retries: int = CONTAINER_MAXIMUM_RETRY_COUNT,
) -> None:
    @retry(
        stop=stop_after_attempt(retries),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def try_connect() -> None:
        logger.info(f"Attempting to connect to database '{database}'...")
        function()
        logger.info(f"Successfully connected to database '{database}'")

    try:
        try_connect()
    except Exception as exception:
        logger.error(
            f"Failed to connect to database '{database}' "
            f"after {retries} attempts: {exception}"
        )
        raise


def ensure_readiness(
    container: Container,
    message: str,
    retries: int = CONTAINER_MAXIMUM_RETRY_COUNT,
) -> None:
    @retry(
        stop=stop_after_attempt(retries),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def try_read() -> None:
        logger.info(
            f"Checking for message '{message}' in container "
            f"'{container.name}' logs..."
        )
        logs = container.logs().decode("utf-8", errors="replace")
        if message not in logs:
            raise RuntimeError(
                f"The message '{message}' is not yet in container "
                f"'{container.name}' logs"
            )
        logger.info(f"Found message '{message}' in container '{container.name}' logs")

    try:
        try_read()
    except Exception as exception:
        logger.error(
            f"Failed to find message '{message}' in container "
            f"'{container.name}' logs after {retries} attempts: {exception}"
        )
        raise


def obtain_prefix(
    fixture_request: pytest.FixtureRequest,
    marker_name: str,
    default_prefix: str,
    valid_prefixes: list[str],
) -> str:
    marker = fixture_request.node.get_closest_marker(marker_name)
    prefix = (
        marker.kwargs.get(ENVIRONMENT_PREFIX, default_prefix)
        if marker
        else default_prefix
    )
    if prefix not in valid_prefixes:
        raise Exception(
            f"The prefix '{prefix}' is not valid. Valid prefixes are: "
            f"{', '.join(valid_prefixes)}. If you want to extend the list "
            f"of valid prefixes, please modify the corresponding "
            f"VALID_*_PREFIXES variable."
        )
    return prefix


def obtain_environment(
    env_prefix: str,
    keys: list[str],
    fixture_name: str,
) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in keys:
        env_name = f"{env_prefix}_{key}"
        value = os.environ.get(env_name)
        if not value:
            raise Exception(
                f"The environment variable {env_name} is not set. "
                f"Unable to run tests using the '{fixture_name}' fixture."
            )
        values[key] = value
    return values


# --- CDC-DB2 constants -----------------------------------------------

DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME: str = (
    f"{CDC_CONTAINER_PREFIX}db2-cdc"
)

DB2_CDC_FORWARDED_PORT: str = "50000/tcp"
DB2_CDC_IMAGE: str = "icr.io/db2_community/db2:latest"
DB2_CDC_DOCKER_DONE_MESSAGE: str = "Setup has completed."
DB2_CDC_TABSDATA_SETUP_DONE_MESSAGE: str = "tabsdata container setup finished"
DB2_CDC_TABSDATA_SETUP_DONE_MARKER: str = "/tmp/tabsdata_setup_done"

DB2_CDC_ROOT_USER: str = "db2inst1"
DB2_CDC_ROOT_PASSWORD: str = DB_PASSWORD

# Database name -- the legacy CDC suite uses ``tdcdcdb`` but the
# new connector reuses the existing ``TESTDB`` provisioned by
# ``db2_config``. Override via env var if needed.
DB2_CDC_DATABASE: str = os.environ.get("DB2_CDC_DATABASE", "TESTDB")

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


# --- Modes + config dataclasses --------------------------------------


class CdcMode(Enum):
    SOFT = auto()
    HARD = auto()


@dataclass
class Db2CdcConfig:
    """Docker-backed CDC config -- carries the live container handle
    so per-test operations (``register_table_for_cdc``, watchdog
    restart, ...) can ``exec_run`` inside it. Independent of (but
    compatible with) ``conftest.DBConfig``, which is purely host:port
    metadata."""

    container: Container | None
    host: str
    port: int
    database: str
    root_user: str
    root_password: str
    cdc_user: str
    cdc_password: str


# --- Db2 connection helpers ------------------------------------------


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
    home = db2_exec(container, "printf '%s' \"$HOME\"").strip()
    if not home:
        raise RuntimeError("Unable to determine Db2 home folder inside container")
    logger.info(f"Db2 home folder: '{home}'")
    return home


# --- One-shot setup steps --------------------------------------------


def db2_archive_already_configured(port: int) -> bool:
    """``logarchmeth1 = DISK:...`` means archive logging is on. The
    persistent ``backup`` folder will also be present on disk, but
    the cfg check is enough to gate the (slow) offline-backup step."""
    try:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                "select value from sysibmadm.dbcfg "
                "where name = 'logarchmeth1'"
            )
            row = cursor.fetchone()
            return bool(
                row and row[0] and row[0].strip().upper().startswith("DISK:")
            )
    except Exception:
        return False


def setup_db2_archive(container: Container, port: int | None = None) -> None:
    """Switch the DB to archive logging + run a baseline backup --
    asncap refuses to start without an archive-log-enabled database.

    Offline backup needs the DB deactivated. The legacy fixture got
    that for free (fresh ``tdcdcdb`` never touched before backup),
    but conn-db2 reuses the ``db2_config`` container whose ``TESTDB``
    already had connections from the seed + concurrency-tune steps,
    so we explicitly ``force applications all`` + ``deactivate`` here.

    When ``port`` is given the function is idempotent -- if
    ``logarchmeth1`` is already ``DISK:...`` the whole archive +
    backup chain is skipped.
    """
    if port is not None and db2_archive_already_configured(port):
        logger.info("Db2 archive logging already configured; skipping")
        return
    db2_exec(
        container,
        f"mkdir -p $HOME/{DB2_CDC_ARCHIVE_FOLDER} $HOME/{DB2_CDC_BACKUP_FOLDER}/",
    )
    db2_exec(
        container,
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 update db cfg for {DB2_CDC_DATABASE} "
        f"using logarchmeth1 disk:$HOME/{DB2_CDC_ARCHIVE_FOLDER}/ && "
        f"db2 connect reset && "
        f"db2 terminate && "
        f"db2 force applications all && "
        # ``force applications`` is async; the deactivate may race it.
        # 5s is what IBM's own docs suggest for small instances.
        f"sleep 5 && "
        f"db2 deactivate database {DB2_CDC_DATABASE} && "
        f"db2 backup db {DB2_CDC_DATABASE} to $HOME/{DB2_CDC_BACKUP_FOLDER}/",
    )
    logger.info("Db2 archive log + baseline backup configured")


def asn_schema_is_provisioned(port: int) -> bool:
    """Heuristic: full asnclp install creates ``IBMSNAP_CAPPARMS``,
    so its presence means a previous session already provisioned
    the apparatus. Synthetic-ASN tests only create
    ``IBMSNAP_REGISTER`` / ``IBMSNAP_UOW`` and never touch
    ``IBMSNAP_CAPPARMS``, so this discriminates the two."""
    try:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select 1 from syscat.tables "
                f"where tabschema = '{DB2_CDC_ASN_SCHEMA}' "
                f"and tabname = 'IBMSNAP_CAPPARMS'"
            )
            return cursor.fetchone() is not None
    except Exception:
        return False


def drop_asn_schema_tables(port: int) -> None:
    """Drop every table in the ASN schema (synthetic or asnclp-made),
    so a subsequent ``asnclp create control tables`` starts from
    scratch. Idempotent."""
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"select tabname from syscat.tables "
            f"where tabschema = '{DB2_CDC_ASN_SCHEMA}'"
        )
        tables = [row[0].strip() for row in cursor.fetchall()]
    if not tables:
        logger.info(f"{DB2_CDC_ASN_SCHEMA} schema has no tables to drop")
        return
    logger.info(
        f"Dropping {len(tables)} table(s) from {DB2_CDC_ASN_SCHEMA} "
        f"schema: {tables}"
    )
    with db2_cursor(port) as (cursor, _):
        for tname in tables:
            try:
                cursor.execute(
                    f"drop table {DB2_CDC_ASN_SCHEMA}.{tname}"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"Failed to drop {DB2_CDC_ASN_SCHEMA}.{tname}: {exc}"
                )


def setup_cdc_db2_asn(container: Container) -> None:
    """Run ``asnclp create control tables for capture server`` to
    materialise the ``ASN.IBMSNAP_*`` registry that asncap reads
    from and writes to."""
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
        f"| asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {control_script} && "
        f"db2 terminate",
    )
    logger.info("ASN control tables created")


def setup_cdc_db2_asncap(container: Container, port: int) -> None:
    """Configure ``IBMSNAP_CAPPARMS``, start the asncap process,
    wait for it to come up + populate ``IBMSNAP_RESTART``."""
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"update {DB2_CDC_ASN_SCHEMA}.ibmsnap_capparms "
            "set capture_path = ?, autostop = 'N', term = 'N'",
            (get_db2_home(container),),
        )
    logger.info("asncap capture parameters updated")

    q_asncap_inits = count_asncap_inits(port)
    start_asncap(container)
    wait_asncap_responsive(container)
    wait_asncap_initialized(port, q_asncap_inits)

    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def wait_for_restart_row() -> None:
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

    wait_for_restart_row()
    logger.info("asncap setup completed")


# --- asncap process status ------------------------------------------


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


# --- CAPSTART / CAPSTOP signal lifecycle -----------------------------


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
    def insert_pruncntl_and_signal() -> str:
        i_map_id = str(random.randint(1, 2**31 - 1))
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"insert into {DB2_CDC_ASN_SCHEMA}.ibmsnap_pruncntl "
                f"(target_server, target_owner, target_table, source_owner, "
                f" source_table, source_view_qual, apply_qual, set_name, "
                f" cntl_server, target_structure, phys_change_owner, "
                f" phys_change_table, map_id) "
                f"values ('{DB2_CDC_DATABASE}', '{DB2_CDC_ASN_SCHEMA}', "
                f"        '{staging_table}', ?, ?, 0, 'TDQUAL', 'TDSET', "
                f"        '{DB2_CDC_DATABASE}', 1, '{DB2_CDC_ASN_SCHEMA}', "
                f"        '{staging_table}', ?)",
                (schema.upper(), table.upper(), i_map_id),
            )
            cursor.execute(
                f"insert into {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"(signal_type, signal_subtype, signal_input_in, signal_state) "
                f"values ('CMD', 'CAPSTART', ?, 'P')",
                (i_map_id,),
            )
        return i_map_id

    map_id = insert_pruncntl_and_signal()
    source_table = f"{schema.upper()}.{table.upper()}"
    logger.info(f"CAPSTART signal inserted for {source_table} (map_id={map_id})")
    return map_id


def wait_capstart_consumed(port: int, map_id: str) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select signal_state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' and signal_subtype = 'CAPSTART' "
                f"and signal_input_in = ? "
                f"order by signal_time desc fetch first 1 row only",
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
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select state from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                f"where source_owner = ? and source_table = ?",
                (schema.upper(), table.upper()),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    f"Registration for {source_table} not found"
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
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check() -> None:
        with db2_cursor(port) as (cursor, _):
            cursor.execute(
                f"select signal_state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' and signal_subtype = 'CAPSTOP' "
                f"and signal_input_in = ? "
                f"order by signal_time desc fetch first 1 row only",
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
                f"select count(*) from {DB2_CDC_ASN_SCHEMA}.ibmsnap_signal "
                f"where signal_type = 'CMD' and signal_subtype = 'CAPSTART' "
                f"and signal_state = 'P' and signal_time < "
                f"current timestamp - {ASNCAP_STALE_SIGNAL_SECONDS} seconds"
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


# --- asncap process lifecycle ----------------------------------------


def start_asncap(container: Container) -> None:
    db2_exec(
        container,
        f"nohup asncap capture_server={DB2_CDC_DATABASE} "
        f"capture_schema={DB2_CDC_ASN_SCHEMA} > /dev/null 2>&1 &",
    )
    logger.info("asncap start command issued")


def wait_asncap_responsive(container: Container) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check() -> None:
        if not is_asncap_responsive(container):
            raise RuntimeError("asncap is not yet responsive")

    check()
    logger.info("asncap is responsive")


def count_asncap_inits(port: int) -> int:
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"select count(*) from {DB2_CDC_ASN_SCHEMA}.ibmsnap_captrace "
            f"where description like '%ASN0109I%'"
        )
        row = cursor.fetchone()
        return row[0] if row else 0


def wait_asncap_initialized(port: int, previous_q_asncap_inits: int) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check_trace() -> None:
        current = count_asncap_inits(port)
        if current <= previous_q_asncap_inits:
            raise RuntimeError(
                f"asncap init trace (ASN0109I) not yet written "
                f"(previous={previous_q_asncap_inits}, current={current})"
            )

    check_trace()
    logger.info("asncap initialization completed (ASN0109I)")


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
        logger.info("asncap stopped gracefully")
        return

    container.exec_run(
        ["bash", "-l", "-c", "pkill -f asncap"],
        user="db2inst1",
    )
    logger.warning("Sent SIGTERM to asncap")
    if wait_asncap_dead(container):
        logger.info("asncap terminated after SIGTERM")
        return

    container.exec_run(
        ["bash", "-l", "-c", "pkill -9 -f asncap"],
        user="db2inst1",
    )
    logger.warning("Sent SIGKILL to asncap")
    time.sleep(5)
    if not is_asncap_alive(container):
        logger.info("asncap killed after SIGKILL")
    else:
        logger.error("asncap still alive after SIGKILL")


def reinit_asncap(container: Container) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
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
    logger.info("asncap reinit completed")


def ensure_asncap_running(container: Container) -> None:
    if is_asncap_responsive(container):
        return

    if is_asncap_alive(container):
        logger.info(
            "asncap exists but is not responsive; "
            f"waiting up to {ASNCAP_STATUS_WAIT_SECONDS}s for it to recover..."
        )

        @retry(
            stop=stop_after_attempt(ASNCAP_STATUS_WAIT_SECONDS),
            wait=wait_incrementing(start=1, increment=1),
            reraise=True,
        )
        def wait_for_recovery() -> None:
            if not is_asncap_responsive(container):
                raise RuntimeError("asncap is still not responsive")

        try:
            wait_for_recovery()
            logger.info("asncap became responsive")
            return
        except RuntimeError:
            logger.warning(
                "asncap exists but never became responsive; will attempt restart"
            )

    logger.warning("asncap is down; restarting...")
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


# --- AsncapWatchdog --------------------------------------------------


class AsncapWatchdog:
    """Background thread that periodically checks the asncap process
    is healthy + actively consuming signals. Restarts + reinits when
    it sees a downed or zombie capture process."""

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
        logger.info("asncap watchdog started")
        while not self._stop_event.wait(ASNCAP_WATCHDOG_INTERVAL_SECONDS):
            try:
                with asncap_lock:
                    if self._stop_event.is_set():
                        break
                    if not is_asncap_responsive(self._container):
                        logger.warning(
                            "watchdog: asncap is down; restarting + reiniting..."
                        )
                        restart_asncap(self._container, self._port)
                        reinit_asncap(self._container)
                    elif has_stale_pending_signals(self._port):
                        logger.warning(
                            "watchdog: zombie asncap (responding but not "
                            "consuming signals); restarting + reiniting..."
                        )
                        restart_asncap(self._container, self._port)
                        reinit_asncap(self._container)
            except Exception as exc:
                logger.warning(f"watchdog health check failed: {exc}")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        logger.info("asncap watchdog stopping")
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


# --- Per-table register / deregister / reregister --------------------


def _add_logmarker_to_cd_table(
    port: int, staging_schema: str, staging_table: str
) -> None:
    """Add the ``IBMSNAP_LOGMARKER`` column to a CD table + a BEFORE
    INSERT trigger that fills it from ``IBMSNAP_UOW``.

    The connector's ingester reads ``IBMSNAP_LOGMARKER`` directly
    from the CD table to get the per-row commit timestamp (cheaper
    than joining ``IBMSNAP_UOW`` on every poll). DB2 11.5 community
    ``asnclp`` has no ``OPTIONS LOGMARKER YES`` clause, and
    ``asncap`` does NOT auto-populate the column even when it
    exists (only ``IBMSNAP_OPERATION`` / ``IBMSNAP_COMMITSEQ`` /
    ``IBMSNAP_INTENTSEQ`` get that treatment).

    The trigger fires before asncap commits each row. ``asncap``
    inserts the matching ``IBMSNAP_UOW`` row first inside the same
    transaction, so the subquery against ``IBMSNAP_UOW`` finds the
    commit timestamp by ``IBMSNAP_COMMITSEQ``. ``COALESCE`` to
    ``CURRENT TIMESTAMP`` is a safety net for any out-of-order
    insertion or refresh paths."""
    trigger_name = f"LM_{staging_table}"
    with db2_cursor(port) as (cursor, _):
        cursor.execute(
            f"alter table {staging_schema}.{staging_table} "
            f"add column IBMSNAP_LOGMARKER TIMESTAMP NOT NULL "
            f"WITH DEFAULT CURRENT TIMESTAMP"
        )
        cursor.execute(
            f"create trigger {staging_schema}.{trigger_name} "
            f"no cascade before insert on "
            f"{staging_schema}.{staging_table} "
            f"referencing new as N "
            f"for each row "
            f"set N.IBMSNAP_LOGMARKER = coalesce("
            f"  (select IBMSNAP_LOGMARKER from "
            f"   {DB2_CDC_ASN_SCHEMA}.IBMSNAP_UOW "
            f"   where IBMSNAP_COMMITSEQ = N.IBMSNAP_COMMITSEQ), "
            f"  CURRENT TIMESTAMP"
            f")"
        )
    logger.info(
        f"Added IBMSNAP_LOGMARKER column + UOW-lookup trigger "
        f"on {staging_schema}.{staging_table}"
    )


def register_table_for_cdc(
    container: Container,
    port: int,
    schema: str,
    table: str,
    ddl: str,
    wait: bool = True,
) -> None:
    """Create the source table, mark it ``data capture changes``,
    run ``asnclp create registration`` to materialise the CD table,
    add the ``IBMSNAP_LOGMARKER`` reserved column (see
    :func:`_add_logmarker_to_cd_table`), then fire CAPSTART + wait
    for the registration to go ``Active``."""
    source_table = f"{schema.upper()}.{table.upper()}"
    staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}"
    register_script = f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}".upper()
    with db2_cursor(port) as (cursor, _):
        cursor.execute(ddl)
        cursor.execute(f"alter table {schema}.{table} data capture changes")
    logger.info(f"Created {source_table} with data capture enabled")

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
        f"options capture all\\n"
        f"quit\\n"
        f"' "
        f"| asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {register_script} && "
        f"db2 terminate",
    )
    logger.info(f"Registration of {source_table} for data capture finished")

    _add_logmarker_to_cd_table(port, DB2_CDC_ASN_SCHEMA, staging_table)

    map_id = send_capstart_signal(port, schema, table, staging_table)

    if wait:
        wait_capstart_consumed(port, map_id)
        wait_registration_active(port, schema, table)

    logger.info(f"Registration of {source_table} complete")


def deregister_table_from_cdc(
    container: Container,
    port: int,
    schema: str,
    table: str,
    mode: CdcMode = CdcMode.HARD,
) -> None:
    """Tear down a registration. ``SOFT`` signals CAPSTOP first
    (clean shutdown); ``HARD`` just rips the rows out and reinits
    asncap."""
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
        cursor.execute(f"alter table {schema}.{table} data capture none")
        connection.commit()
        match mode:
            case CdcMode.HARD:
                cursor.execute(
                    f"delete from {DB2_CDC_ASN_SCHEMA}.ibmsnap_pruncntl "
                    f"where source_owner = ? and source_table = ?",
                    (schema.upper(), table.upper()),
                )
                cursor.execute(
                    f"delete from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                    f"where source_owner = ? and source_table = ?",
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
                    f"where source_owner = ? and source_table = ?",
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
        f"Deregistration ({mode.name}) of {source_table} complete"
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
    """Re-enable data capture on a previously-deregistered table.
    ``SOFT`` keeps history by stamping a new staging table name
    with ``_V{version}``; ``HARD`` reuses the original staging
    table name."""
    source_table = f"{schema.upper()}.{table.upper()}"
    match mode:
        case CdcMode.HARD:
            staging_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}"
            register_script = (
                f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}_rereg".upper()
            )
        case CdcMode.SOFT:
            staging_table = (
                f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.upper()}_V{version}"
            )
            register_script = (
                f"{DB2_CDC_REGISTER_SCRIPT}_{table.lower()}_v{version}".upper()
            )
        case _:
            raise ValueError(f"Unknown CDC mode: {mode}")

    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(f"alter table {schema}.{table} data capture changes")
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
        f"options logmarker yes\\n"
        f"quit\\n"
        f"' "
        f"| asnclp && "
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 -tvf {register_script} && "
        f"db2 terminate",
    )
    logger.info(f"Re-registration of {source_table} finished")

    map_id = send_capstart_signal(port, schema, table, staging_table)

    if wait:
        wait_capstart_consumed(port, map_id)
        wait_registration_active(port, schema, table)

    logger.info(
        f"Re-registration ({mode.name}) of {source_table} complete "
        f"(staging={staging_table})"
    )

    if mode == CdcMode.SOFT:
        return f"{DB2_CDC_ASN_SCHEMA.lower()}.{staging_table}"
    return None


# --- Signal-table smoke check ----------------------------------------


def setup_cdc_db2_signal(container: Container, port: int) -> str:
    """Create a randomly-named CDC-registered signal table and
    populate it with three rows. Its name is the witness used by
    :func:`check_cdc_db2_container` to confirm the whole pipeline
    works end-to-end."""
    table_suffix = "".join(
        random.choices(string.ascii_uppercase + string.digits, k=16)
    )
    table_name = f"{DB2_CDC_SIGNAL_TABLE}_{table_suffix}"

    register_table_for_cdc(
        container=container,
        port=port,
        schema=DB2_CDC_ROOT_USER,
        table=table_name,
        ddl=(
            f"create table {table_name} ("
            f"id int generated by default as identity, "
            f"name varchar(255), "
            f"code int"
            f")"
        ),
        wait=True,
    )
    logger.info(f"Created signal table '{table_name}'")

    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        for name, code in [
            ("Go Seigen", 21),
            ("Lee Sedol", 17),
            ("Kitani Minoru", 19),
        ]:
            cursor.execute(
                f"insert into {table_name} (name, code) values "
                f"('{name}', {code})"
            )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    logger.info(f"Seeded signal table '{table_name}'")
    return table_name


def check_cdc_db2_log(port: int) -> None:
    connection = None
    cursor = None
    try:
        connection = db2_conn(port)
        cursor = connection.cursor()
        cursor.execute(
            "select value from sysibmadm.dbcfg where name = 'logarchmeth1'"
        )
        row = cursor.fetchone()
        if not row or not row[0] or not row[0].strip().upper().startswith("DISK:"):
            raise RuntimeError(
                f"Db2 CDC check failed: logarchmeth1 is not DISK (got {row})"
            )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    logger.info("Db2 archive configuration check passed")


def check_cdc_db2_asn_registration(port: int, signal: str) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def check() -> None:
        connection = None
        cursor = None
        try:
            connection = db2_conn(port)
            cursor = connection.cursor()
            cursor.execute(
                "select source_owner, source_table, cd_owner, cd_table, state "
                f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
                f"where source_table = '{signal.upper()}'"
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    f"{signal} not in {DB2_CDC_ASN_SCHEMA}.ibmsnap_register"
                )
            registration_state = row[4].strip()
            if registration_state.upper() != "A":
                raise RuntimeError(
                    f"Registration for '{signal}' is not active "
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
            f"Registration for '{signal}' did not become active "
            f"after {DB2_WAIT_TIMEOUT_SECONDS}s: {exception}"
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
            raise RuntimeError(f"Signal table '{signal}' has no data")

        cursor.execute(
            f"select cd_owner, cd_table "
            f"from {DB2_CDC_ASN_SCHEMA}.ibmsnap_register "
            f"where source_table = '{signal.upper()}'"
        )
        row = cursor.fetchone()
        signal_cd_owner = row[0].strip()
        signal_cd_table = row[1].strip()
        expected_cd_table = f"{DB2_CDC_STAGING_TABLE_PREFIX}{signal.upper()}"
        if signal_cd_table != expected_cd_table:
            raise RuntimeError(
                f"Staging table is '{signal_cd_table}' "
                f"but expected '{expected_cd_table}'"
            )

        @retry(
            stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
            wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
            reraise=True,
        )
        def wait_for_captured_data() -> None:
            cursor.execute(
                f"select count(*) from {signal_cd_owner}.{signal_cd_table}"
            )
            cd_count = cursor.fetchone()[0]
            logger.info(
                f"Staging table {signal_cd_owner}.{signal_cd_table} has "
                f"{cd_count} captured rows"
            )
            if cd_count == 0:
                raise RuntimeError(
                    f"Staging table {signal_cd_owner}.{signal_cd_table} "
                    f"has no captured data"
                )

        wait_for_captured_data()

        cursor.execute(
            f"select colname from syscat.columns "
            f"where tabschema = '{signal_cd_owner}' "
            f"and tabname = '{signal_cd_table}' "
            f"order by colno"
        )
        cd_columns = [row[0].strip() for row in cursor.fetchall()]
        if not cd_columns:
            raise RuntimeError(
                f"No columns for {signal_cd_owner}.{signal_cd_table}"
            )
        for expected in ["ID", "NAME", "CODE"]:
            if expected not in cd_columns:
                raise RuntimeError(
                    f"Source column '{expected}' missing in "
                    f"{signal_cd_owner}.{signal_cd_table} "
                    f"(columns: {cd_columns})"
                )

        for expected in ["ID", "NAME", "CODE"]:
            old_col = f"{DB2_CDC_OLD_VALUES_PREFIX}{expected}"
            if old_col not in cd_columns:
                raise RuntimeError(
                    f"Old-value column '{old_col}' missing in "
                    f"{signal_cd_owner}.{signal_cd_table} "
                    f"(columns: {cd_columns})"
                )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logger.info(
        f"asn operation verification for signal table '{signal}' passed"
    )


# --- Standalone container lifecycle (legacy parity) ------------------
#
# These functions create + manage a dedicated CDC container (``tdcdcdb``
# database). The conn-db2 happy path layers CDC on top of the existing
# ``db2_config`` container instead, but the standalone variant stays
# here so external suites can opt into the legacy behaviour.


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


def log_tabsdata_setup_done(container: Container) -> None:
    container.exec_run(
        [
            "bash",
            "-c",
            f"echo '{DB2_CDC_TABSDATA_SETUP_DONE_MESSAGE}' "
            f"> {DB2_CDC_TABSDATA_SETUP_DONE_MARKER}",
        ],
    )
    logger.info(
        f"Created setup marker '{DB2_CDC_TABSDATA_SETUP_DONE_MARKER}'"
    )


def wait_for_tabsdata_setup_done(container: Container) -> None:
    @retry(
        stop=stop_after_delay(DB2_WAIT_TIMEOUT_SECONDS),
        wait=wait_fixed(DB2_WAIT_POLL_INTERVAL_SECONDS),
        reraise=True,
    )
    def try_read() -> None:
        logger.info(
            f"Checking for setup marker '{DB2_CDC_TABSDATA_SETUP_DONE_MARKER}' "
            f"in container '{container.name}'..."
        )
        try:
            container.get_archive(DB2_CDC_TABSDATA_SETUP_DONE_MARKER)
        except docker.errors.NotFound:
            raise RuntimeError(
                f"The marker '{DB2_CDC_TABSDATA_SETUP_DONE_MARKER}' "
                f"does not yet exist in container '{container.name}'"
            )
        logger.info(
            f"Found setup marker '{DB2_CDC_TABSDATA_SETUP_DONE_MARKER}' "
            f"in container '{container.name}'"
        )

    try:
        try_read()
    except Exception as exception:
        logger.error(
            f"Failed to find setup marker '{DB2_CDC_TABSDATA_SETUP_DONE_MARKER}' "
            f"in container '{container.name}' "
            f"after {DB2_WAIT_TIMEOUT_SECONDS}s: {exception}"
        )
        raise


def resize_db2_buffer_pool(container: Container) -> None:
    logger.info("Resizing DB2 CDC buffer pool to 256 MB")
    output = db2_exec(
        container,
        f"db2 connect to {DB2_CDC_DATABASE} && "
        f"db2 alter bufferpool ibmdefaultbp size 65536",
    )
    logger.info(f"DB2 CDC buffer pool resized to 256 MB: {output}")


def setup_cdc_db2_container(container: Container, port: int) -> str:
    """Composite of the full CDC bring-up: archive + asn + asncap
    + signal table. Returns the signal table name so the caller
    can verify the pipeline."""
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
        resize_db2_buffer_pool(container)
        log_tabsdata_setup_done(container)
    else:
        wait_for_tabsdata_setup_done(container)
    return container, port


# --- Db2CdcConfig fixture (legacy parity) ----------------------------


@pytest.fixture(scope="function")
def db2_cdc_config(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> Db2CdcConfig:
    """Standalone fixture that boots a dedicated CDC container,
    mirroring the legacy ``test_tabsdata_cdc_db2`` fixture of the
    same name. The conn-db2 happy path uses
    :func:`conftest.fresh_db2_cdc_asncap` instead, which rides on
    top of the existing ``db2_config`` container."""
    prefix = obtain_prefix(
        request,
        DB2_CDC_MARKER,
        DB2_CDC_DEFAULT_PREFIX,
        VALID_DB2_CDC_PREFIXES,
    )
    env_prefix = f"{prefix}__{DB2_CDC_ENV_SUFFIX}"
    if os.environ.get(f"{env_prefix}_ENVS") in TRUE_VALUES:
        envs = obtain_environment(
            env_prefix, DB2_CDC_REQUIRED_ENVS, "db2_cdc_config",
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
            cdc_user=envs["ROOT_USER"],
            cdc_password=envs["ROOT_PASSWORD"],
        )

    name = os.environ.get(
        f"{env_prefix}_DOCKER_CONTAINER_NAME",
        DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME,
    )
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


# --- Container lookup helper for the conn-db2 hybrid path ------------


def find_container_by_host_port(host_port: int) -> Container | None:
    """Locate a running Docker container by host port binding.

    The conn-db2 ``db2_config`` fixture only exposes ``host`` + ``port``,
    not the Container handle. This helper walks ``docker ps`` and
    matches the published port on ``50000/tcp`` so the asncap helpers
    (which need ``exec_run`` inside the container) can be layered on
    top of ``db2_config`` without rewiring the fixture."""
    client = docker.from_env()
    for c in client.containers.list():
        bindings = (
            c.attrs.get("NetworkSettings", {})
            .get("Ports", {})
            .get(DB2_CDC_FORWARDED_PORT, [])
        ) or []
        for binding in bindings:
            try:
                if int(binding.get("HostPort", 0)) == host_port:
                    return c
            except (TypeError, ValueError):
                continue
    return None
