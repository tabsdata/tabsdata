#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import random
import re
import string
import time
import urllib.parse
from dataclasses import dataclass
from typing import Iterator

from filelock import FileLock

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdatak.conn.db2.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from test_tabsdata.common.constants import (
    CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS,
    CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS,
    DOCKER_REGULAR_CONTAINER_INFIX,
    DOCKER_UTEST_CONTAINER_INFIX,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


# --- Var var gates --------------------------------------------------

# Anything in TRUE_VALUES (case-insensitive) opts out of teardown.
TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"

# Default: failures starting docker / pulling images / running the
# container raise so CI surfaces the problem. Set this var truthy
# to convert those failures into ``pytest.skip`` -- useful on a
# laptop without docker.
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

# External-DB var prefixes. ``{prefix}__{backend}_ENVS`` truthy
# switches the fixture from docker mode to external mode.
_DB2_PREFIX = "DB20"
_DB2_BACKEND = "DB2"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

_DB2_IMAGE = "icr.io/db2_community/db2:11.5.9.0"
_DB2_PLATFORM = "linux/amd64"
_DB2_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "db2"
)

_DB_NAME = "TESTDB"
# Db2 community image's default instance owner; the image creates
# this account during init and grants it the DBADM authority.
_DB_USER = "db2inst1"
_DB_PASSWORD = "p@ssw0rd"

# Db2 takes ~3 minutes for the first init (database creation,
# user creation, table-space allocation). Allow generous polling.
_READY_MAX_RETRIES = 120
_READY_BACKOFF_S = 4


@dataclass(frozen=True)
class DBConfig:

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def uri(self) -> str:
        return f"db2+ibm_db://{self.host}:{self.port}/{self.database}"

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        return (
            f"db2+ibm_db://{u}:{p}@{self.host}:{self.port}/{self.database}"
        )

    @property
    def credentials(self) -> UserPassword:
        return UserPassword(
            user=Secret(self.user),
            password=Secret(self.password),
        )


# --- Docker helpers -------------------------------------------------


def _require_docker():
    try:
        import docker
    except ImportError:
        _fail_or_skip("docker SDK not installed (`pip install docker`)")
        return
    try:
        client = docker.from_env()
        client.ping()
    except Exception as exc:  # noqa: BLE001 -- broad on purpose
        _fail_or_skip(f"docker daemon unreachable: {exc}")
    return client


def _ensure_container(
    *,
    name: str,
    image: str,
    environment: list[str],
) -> int:
    from docker.errors import APIError, ImageNotFound, NotFound

    client = _require_docker()

    try:
        existing = client.containers.get(name)
    except NotFound:
        existing = None

    if existing is not None:
        if existing.status != "running":
            existing.start()
        host_port = _wait_for_container_port(existing)
        if host_port is None:
            existing.remove(force=True)
            existing = None
        else:
            return host_port

    try:
        client.images.get(image)
    except ImageNotFound:
        try:
            logger.info("pulling docker image %s (~3GB pull)", image)
            client.images.pull(image, platform=_DB2_PLATFORM)
        except APIError as exc:
            _fail_or_skip(f"docker image {image!r} pull failed: {exc}")

    try:
        # Db2 needs --privileged for namespace-tweaking + LICENSE=accept
        # passed as an env var or it refuses to start.
        container = client.containers.run(
            image,
            name=name,
            environment=environment,
            ports={"50000/tcp": None},
            privileged=True,
            detach=True,
        platform=_DB2_PLATFORM,
        )
    except APIError as exc:
        _fail_or_skip(f"docker run {image!r} failed: {exc}")

    host_port = _wait_for_container_port(container)
    if host_port is None:
        container.remove(force=True)
        _fail_or_skip("docker container started but published no host port")
    return host_port


def _container_port(container) -> int | None:
    ports = (container.attrs.get("NetworkSettings") or {}).get("Ports") or {}
    bindings = ports.get("50000/tcp") or []
    if not bindings:
        return None
    try:
        return int(bindings[0]["HostPort"])
    except (KeyError, ValueError, IndexError):
        return None


def _wait_for_container_port(container) -> int | None:
    deadline = time.monotonic() + CONTAINER_PORT_BINDING_WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        container.reload()
        port = _container_port(container)
        if port is not None:
            return port
        time.sleep(CONTAINER_PORT_BINDING_WAIT_POLL_INTERVAL_SECONDS)
    return None


def _tune_db2_for_concurrency(container_name: str, db: str) -> None:
    if _dont_kill():
        return
    try:
        import docker
        from docker.errors import APIError, NotFound
    except ImportError:
        return
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
    except (NotFound, APIError, Exception):  # noqa: BLE001
        return
    # MAX_CONNECTIONS / MAX_COORDAGENTS / MAXAPPLS are configurable online;
    # no db2stop/db2start needed. The restart used to put TESTDB into
    # BACKUP PENDING state on the IBM community image (SQL1116N).
    tune_script = (
        "su - db2inst1 -c '"
        "db2 update dbm cfg using MAX_CONNECTIONS 512 MAX_COORDAGENTS 512"
        f" && db2 connect to {db}"
        " && db2 update db cfg using MAXAPPLS 512"
        " && db2 connect reset"
        " && db2 terminate"
        "'"
    )
    try:
        exit_code, output = container.exec_run(
            ["bash", "-lc", tune_script], demux=False,
        )
        if exit_code != 0:
            logger.warning(
                "Db2 concurrency tuning returned non-zero (%s): %s",
                exit_code, output.decode(errors="replace") if output else "",
            )
    except APIError as exc:
        logger.warning("Db2 concurrency tuning skipped (docker error): %s", exc)


def _wait_for_db(host: str, port: int, user: str, password: str, db: str) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import OperationalError

    u = urllib.parse.quote(user, safe="")
    p = urllib.parse.quote(password, safe="")
    url = f"db2+ibm_db://{u}:{p}@{host}:{port}/{db}"

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            engine = create_engine(url)
            try:
                with engine.connect() as conn:
                    # Db2's "select from no table" form.
                    conn.execute(text("SELECT 1 FROM SYSIBM.SYSDUMMY1"))
                return
            finally:
                engine.dispose()
        except (OperationalError, Exception) as e:  # noqa: BLE001
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"Db2 at {host}:{port}/{db} did not accept connections "
        f"after {_READY_MAX_RETRIES} attempts (last error: {last_error})"
    )


_TRANSIENT_COMMS_MARKERS = ("CLI0108E", "SQLCODE=-99999", "SQL1224N")


def _is_transient_comms_error(exc: BaseException) -> bool:
    message = str(exc)
    return any(marker in message for marker in _TRANSIENT_COMMS_MARKERS)


def run_query_with_retry(query_fn, *, timeout_s: float = 60.0, poll_s: float = 0.5):
    from sqlalchemy.exc import DBAPIError, OperationalError

    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while True:
        try:
            return query_fn()
        except (OperationalError, DBAPIError) as exc:
            if not _is_transient_comms_error(exc):
                raise
            last_error = exc
            if time.monotonic() >= deadline:
                raise
            time.sleep(poll_s)


def _seed_schema(cfg: DBConfig) -> None:
    from sqlalchemy import create_engine, text

    _wait_for_db(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)

    # Db2 has no ``DROP TABLE IF EXISTS``; the compound statement
    # below installs a SQLSTATE '42704' (undefined name) handler so
    # the drop is a no-op when the table is absent.
    drop_if_exists = (
        "BEGIN "
        "DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; "
        "EXECUTE IMMEDIATE 'DROP TABLE {tbl}'; "
        "END"
    )

    engine = create_engine(cfg.uri_with_credentials)

    def _seed():
        with engine.begin() as conn:
            conn.execute(text(drop_if_exists.format(tbl="orders")))
            conn.execute(text(drop_if_exists.format(tbl="users")))
            conn.execute(text(
                "CREATE TABLE users ("
                "  id INTEGER NOT NULL PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  age INTEGER NOT NULL"
                ")"
            ))
            for sql in (
                "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)",
                "INSERT INTO users (id, name, age) VALUES (2, 'bob', 25)",
                "INSERT INTO users (id, name, age) VALUES (3, 'carol', 40)",
            ):
                conn.execute(text(sql))
            conn.execute(text(
                "CREATE TABLE orders ("
                "  id INTEGER NOT NULL PRIMARY KEY,"
                "  user_id INTEGER NOT NULL,"
                "  qty INTEGER NOT NULL"
                ")"
            ))
            for sql in (
                "INSERT INTO orders (id, user_id, qty) VALUES (10, 1, 2)",
                "INSERT INTO orders (id, user_id, qty) VALUES (11, 2, 5)",
            ):
                conn.execute(text(sql))

    try:
        run_query_with_retry(_seed)
    finally:
        engine.dispose()


def _remove_container(name: str) -> None:
    if _dont_kill():
        logger.info(
            "TD_DOCKER_DONT_KILL is set; leaving container %r running",
            name,
        )
        return
    try:
        import docker
        from docker.errors import NotFound

        client = docker.from_env()
        try:
            container = client.containers.get(name)
        except NotFound:
            return
        logger.info("removing container %r", name)
        container.remove(v=True, force=True)
    except Exception as exc:  # noqa: BLE001 -- teardown must not raise
        logger.warning(
            "failed to remove container %r (safe to ignore on a "
            "docker-less machine): %s",
            name, exc,
        )


# --- External-DB var path ---------------------------------------

_HOST_PORT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+._-]*://([^:/?#]+):(\d+)/[^?#]+")


def _required_env(name: str, fixture: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"environment variable {name} is not set. Unable to "
            f"run tests using the {fixture!r} fixture in external-DB "
            f"mode."
        )
    return val


def _external_config(
    *, prefix: str, backend: str, fixture: str,
) -> DBConfig | None:
    if not _truthy(f"{prefix}__{backend}_ENVS"):
        return None

    uri = _required_env(f"{prefix}__{backend}_URI", fixture)
    user = _required_env(f"{prefix}__{backend}_USER", fixture)
    password = _required_env(f"{prefix}__{backend}_PASSWORD", fixture)
    database = _required_env(f"{prefix}__{backend}_DATABASE", fixture)

    match = _HOST_PORT_RE.match(uri)
    if not match:
        raise RuntimeError(
            f"environment variable {prefix}__{backend}_URI={uri!r} is "
            f"not a valid '<scheme>://host:port/db' URI. Unable to "
            f"run tests using the {fixture!r} fixture in external-DB "
            f"mode."
        )
    host, port_s = match.groups()
    return DBConfig(
        host=host,
        port=int(port_s),
        user=user,
        password=password,
        database=database,
    )


def _should_reseed_external(prefix: str, backend: str) -> bool:
    return _truthy(f"{prefix}__{backend}_RESEED")


# --- Public fixtures ------------------------------------------------


def _build_config(
    *,
    name: str,
    image: str,
    environment: list[str],
) -> DBConfig:
    host_port = _ensure_container(
        name=name,
        image=image,
        environment=environment,
    )
    logger.info(
        "_build_config[%s]: ensure_container=%s",
        name, host_port,
    )
    cfg = DBConfig(
        host="127.0.0.1",
        port=host_port,
        user=_DB_USER,
        password=_DB_PASSWORD,
        database=_DB_NAME,
    )
    _wait_for_db(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)
    _tune_db2_for_concurrency(name, cfg.database)
    _wait_for_db(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)
    _seed_schema(cfg)
    return cfg


@pytest.fixture(scope="session")
def db2_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_DB2_PREFIX, backend=_DB2_BACKEND, fixture="db2_config",
    )
    if external is not None:
        if _should_reseed_external(_DB2_PREFIX, _DB2_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_DB2_CONTAINER,
            image=_DB2_IMAGE,
            environment=[
                "LICENSE=accept",
                f"DB2INST1_PASSWORD={_DB_PASSWORD}",
                f"DBNAME={_DB_NAME}",
                "BLU=false",
                "ENABLE_ORACLE_COMPATIBILITY=false",
                "UPDATEAVAIL=NO",
                "TO_CREATE_SAMPLEDB=false",
                "REPODB=false",
                "IS_OSXFS=false",
                "PERSISTENT_HOME=true",
                "HADR_ENABLED=false",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_db2_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_DB2_CONTAINER,
                image=_DB2_IMAGE,
                environment=[
                    "LICENSE=accept",
                    f"DB2INST1_PASSWORD={_DB_PASSWORD}",
                    f"DBNAME={_DB_NAME}",
                    "BLU=false",
                    "ENABLE_ORACLE_COMPATIBILITY=false",
                    "UPDATEAVAIL=NO",
                    "TO_CREATE_SAMPLEDB=false",
                    "REPODB=false",
                    "IS_OSXFS=false",
                    "PERSISTENT_HOME=true",
                    "HADR_ENABLED=false",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_DB2_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``db2_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_db2(db2_config, tmp_path_factory, worker_id) -> DBConfig:
    if worker_id == "master":
        _seed_schema(db2_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_db2_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(db2_config)
    return db2_config


# --- CDC fixtures ---------------------------------------------------
#
# Db2's IBM SQL Replication apparatus is much heavier than mysql's
# binlog or postgres's logical decoding: it relies on the ``asncap``
# capture program populating shadow CD tables via the ASN control
# tables. Reproducing that machinery (asnclp control-table creation,
# asncap watchdog, registration ALTER TABLEs) in a test fixture is
# 1000+ lines and brittle.
#
# Instead the CDC fixture synthesises just the shape the ingester
# reads from: an ``ASN`` schema with the two control tables it
# queries (``IBMSNAP_REGISTER``, ``IBMSNAP_UOW``), a source table,
# and a CD table with the IBMSNAP_* system columns + the user
# columns + their before-image siblings. Integration tests then
# emit synthetic change events by inserting rows into the CD table
# directly. This exercises every code path in the ingester (table
# identity resolution against IBMSNAP_REGISTER, per-table watermark
# polling, CD-column matching via the registered ``before_img_prefix``,
# row emission with mapped ops, offset advance) without needing the
# asncap capture program.

# The CD-table schema used by the integration tests. ``BEFORE_PREFIX``
# matches the value the fixture writes into IBMSNAP_REGISTER.
CDC_BEFORE_PREFIX = "X_"
CDC_SOURCE_SCHEMA = "DB2INST1"
CDC_SOURCE_TABLE = "USERS"
CDC_CD_SCHEMA = "DB2INST1"
CDC_CD_TABLE = "USERS_CD"


def _seed_cdc_schema(cfg: "DBConfig") -> None:
    """Build the IBM SQL Replication apparatus the ingester reads
    from, by hand:

    * ``ASN.IBMSNAP_REGISTER`` -- one row mapping the CD table back
      to the source table, with our chosen before-image prefix.
    * ``ASN.IBMSNAP_UOW`` -- for ``TimestampPosition`` translation
      (the ingester computes
      ``max(IBMSNAP_COMMITSEQ) where IBMSNAP_LOGMARKER <= ts``).
    * Source ``DB2INST1.USERS`` -- visible to the SA inspector so
      ``inspector.has_table(source_owner, source_table)`` passes.
    * CD ``DB2INST1.USERS_CD`` -- IBMSNAP_* + user + before-image
      columns.

    All structures are dropped + recreated per-test so each test
    starts from a clean slate.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(cfg.uri_with_credentials)
    try:
        with engine.begin() as conn:
            # ASN schema -- Db2 has no IF NOT EXISTS for CREATE SCHEMA
            # on this version, so probe SYSCAT.SCHEMATA first.
            row = conn.execute(text(
                "select schemaname from syscat.schemata "
                "where schemaname = 'ASN'"
            )).fetchone()
            if not row:
                conn.execute(text("create schema ASN"))
            # IBMSNAP_REGISTER -- drop / recreate.
            for stmt in (
                "drop table ASN.IBMSNAP_REGISTER",
                f"drop table ASN.IBMSNAP_UOW",
                f"drop table {CDC_CD_SCHEMA}.{CDC_CD_TABLE}",
                f"drop table {CDC_SOURCE_SCHEMA}.{CDC_SOURCE_TABLE}",
            ):
                try:
                    conn.execute(text(stmt))
                except Exception:  # noqa: BLE001 -- table may not exist
                    pass
            conn.execute(text(
                "create table ASN.IBMSNAP_REGISTER ("
                "  SOURCE_OWNER VARCHAR(128) NOT NULL,"
                "  SOURCE_TABLE VARCHAR(128) NOT NULL,"
                "  CD_OWNER VARCHAR(128) NOT NULL,"
                "  CD_TABLE VARCHAR(128) NOT NULL,"
                "  BEFORE_IMG_PREFIX VARCHAR(4)"
                ")"
            ))
            conn.execute(text(
                "create table ASN.IBMSNAP_UOW ("
                "  IBMSNAP_COMMITSEQ VARCHAR(16) FOR BIT DATA NOT NULL,"
                "  IBMSNAP_LOGMARKER TIMESTAMP NOT NULL"
                ")"
            ))
            conn.execute(text(
                f"create table {CDC_SOURCE_SCHEMA}.{CDC_SOURCE_TABLE} ("
                "  ID INTEGER NOT NULL PRIMARY KEY,"
                "  NAME VARCHAR(64) NOT NULL,"
                "  AGE INTEGER NOT NULL"
                ")"
            ))
            conn.execute(text(
                f"create table {CDC_CD_SCHEMA}.{CDC_CD_TABLE} ("
                "  IBMSNAP_OPERATION CHAR(1) NOT NULL,"
                "  IBMSNAP_COMMITSEQ VARCHAR(16) FOR BIT DATA NOT NULL,"
                "  IBMSNAP_INTENTSEQ VARCHAR(16) FOR BIT DATA NOT NULL,"
                "  IBMSNAP_LOGMARKER TIMESTAMP NOT NULL,"
                "  ID INTEGER,"
                "  NAME VARCHAR(64),"
                "  AGE INTEGER,"
                "  X_ID INTEGER,"
                "  X_NAME VARCHAR(64),"
                "  X_AGE INTEGER"
                ")"
            ))
            conn.execute(text(
                "insert into ASN.IBMSNAP_REGISTER "
                "(SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE, "
                " BEFORE_IMG_PREFIX) values "
                f"('{CDC_SOURCE_SCHEMA}', '{CDC_SOURCE_TABLE}', "
                f" '{CDC_CD_SCHEMA}', '{CDC_CD_TABLE}', "
                f" '{CDC_BEFORE_PREFIX}')"
            ))
    finally:
        engine.dispose()


@pytest.fixture
def fresh_db2_cdc(db2_config, tmp_path_factory, worker_id) -> DBConfig:
    """Per-test reset of the ASN + CD schema. Reuses the
    session-scoped ``db2_config`` so the (slow) Db2 container
    boot is paid only once per session."""
    if worker_id == "master":
        _seed_cdc_schema(db2_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_db2_cdc_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_cdc_schema(db2_config)
    return db2_config


# --- Real-asncap CDC fixture ----------------------------------------
#
# Companion to ``fresh_db2_cdc`` (synthetic-ASN). This one provisions
# the real IBM SQL Replication apparatus inside the ``db2_config``
# container: archive logging, ``asnclp`` control tables, the
# ``asncap`` capture program, and an ``AsncapWatchdog`` -- the same
# sequence the legacy ``tabsdata-cdc-db2`` suite runs against its
# dedicated ``tdcdcdb`` container. Tests drive change events by
# inserting into real source tables; asncap propagates them to the
# CD table the way it would in production.

from test_tabsdatak.conn.db2 import _cdc_docker  # noqa: E402


@pytest.fixture(scope="session")
def db2_asncap_ready(db2_config, tmp_path_factory, worker_id):
    """One-shot provisioning of asncap on the ``db2_config`` container.

    Cost: ~30s for archive + backup + asnclp + asncap startup on top of
    the ~3-min ``db2_config`` cold boot. Subsequent tests reuse it.
    Returns a :class:`_cdc_docker.Db2CdcConfig` carrying the live
    Container handle so per-test ``register_table_for_cdc`` calls can
    ``exec_run`` inside it."""
    container = _cdc_docker.find_container_by_host_port(db2_config.port)
    if container is None:
        raise RuntimeError(
            f"Could not locate the docker container behind db2_config "
            f"at host_port={db2_config.port}. asncap setup requires "
            f"exec_run access inside the container."
        )

    # The ported docker helpers default to ``DB2_CDC_DATABASE`` /
    # ``DB2_CDC_ROOT_USER`` / ``DB2_CDC_ROOT_PASSWORD`` (the legacy
    # tdcdcdb credentials from ``test_tabsdata.common.constants``).
    # Rebind them to match the actual ``db2_config`` container so
    # ``db2_conn`` / ``db2_cursor`` authenticate successfully.
    _cdc_docker.DB2_CDC_DATABASE = db2_config.database
    _cdc_docker.DB2_CDC_ROOT_USER = db2_config.user
    _cdc_docker.DB2_CDC_ROOT_PASSWORD = db2_config.password

    def _provision() -> None:
        _cdc_docker.get_db2_home(container)
        _cdc_docker.setup_db2_archive(container, port=db2_config.port)
        # Co-existence with the synthetic-ASN tests (``fresh_db2_cdc``):
        # they leave ``ASN.IBMSNAP_REGISTER`` + ``IBMSNAP_UOW`` behind
        # in the same TESTDB container; asnclp's
        # ``create control tables`` refuses to proceed against any
        # pre-existing ASN tables. Drop the schema clean unless asnclp
        # has already provisioned the full set itself in a prior run.
        if not _cdc_docker.asn_schema_is_provisioned(db2_config.port):
            _cdc_docker.drop_asn_schema_tables(db2_config.port)
            _cdc_docker.setup_cdc_db2_asn(container)
            _cdc_docker.setup_cdc_db2_asncap(container, db2_config.port)
        else:
            logger.info(
                "asnclp control tables already present; reusing them"
            )
            _cdc_docker.ensure_asncap_running(container)

    if worker_id == "master":
        _provision()
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_db2_cdc_asncap_setup.lock"
        with FileLock(str(lock_path)):
            _provision()

    _cdc_docker.AsncapWatchdog.ensure_started(container, db2_config.port)

    cfg = _cdc_docker.Db2CdcConfig(
        container=container,
        host=db2_config.host,
        port=db2_config.port,
        database=db2_config.database,
        root_user=db2_config.user,
        root_password=db2_config.password,
        cdc_user=db2_config.user,
        cdc_password=db2_config.password,
    )
    try:
        yield cfg
    finally:
        _cdc_docker.AsncapWatchdog.shutdown()


@pytest.fixture
def fresh_db2_cdc_asncap(db2_asncap_ready, request):
    """Per-test CDC config that registers a uniquely-named source
    table for the test and tears it down afterwards.

    The test signature looks like ``def test_foo(fresh_db2_cdc_asncap)``;
    the fixture returns ``(cfg, source_schema, source_table)`` so the
    test can drive ``INSERT/UPDATE/DELETE`` against the source and let
    asncap propagate to the CD table the ingester reads from."""
    cfg = db2_asncap_ready
    suffix = "".join(
        random.choices(string.ascii_uppercase + string.digits, k=10)
    )
    source_table = f"USERS_{suffix}"
    source_schema = cfg.root_user.upper()
    ddl = (
        f"create table {source_table} ("
        f"id integer not null primary key, "
        f"name varchar(64) not null, "
        f"age integer not null"
        f")"
    )
    _cdc_docker.register_table_for_cdc(
        container=cfg.container,
        port=cfg.port,
        schema=source_schema,
        table=source_table,
        ddl=ddl,
        wait=True,
    )

    def _teardown() -> None:
        try:
            _cdc_docker.deregister_table_from_cdc(
                container=cfg.container,
                port=cfg.port,
                schema=source_schema,
                table=source_table,
                mode=_cdc_docker.CdcMode.HARD,
            )
        except Exception as exc:  # noqa: BLE001 -- teardown must not raise
            logger.warning(
                f"deregister of {source_schema}.{source_table} "
                f"failed during teardown: {exc}"
            )

    request.addfinalizer(_teardown)
    return cfg, source_schema, source_table
