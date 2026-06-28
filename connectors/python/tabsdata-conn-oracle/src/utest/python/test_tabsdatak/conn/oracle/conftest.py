#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Iterator

from filelock import FileLock

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdatak.conn.oracle.bootest import TESTING_RESOURCES_PATH


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


# --- Secret gates --------------------------------------------------

# Anything in TRUE_VALUES (case-insensitive) opts out of teardown.
TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"

# Default: failures starting docker / pulling images / running the
# container raise so CI surfaces the problem. Set this var truthy
# to convert those failures into ``pytest.skip`` -- useful on a
# laptop without docker.
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

_ORACLE_PREFIX = "ORACLE0"
_ORACLE_BACKEND = "ORACLE"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

# gvenzl/oracle-free is the standard community image for Oracle
# Free 23ai. Starts faster than the official container-registry
# image and ships smaller. Apache 2.0.
_ORACLE_IMAGE = "gvenzl/oracle-free:23-slim"
_ORACLE_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "oracle"
)

# 23ai pluggable database name. Pre-created by the image entrypoint.
_DB_NAME = "FREEPDB1"
_DB_USER = "etl"
_DB_PASSWORD = "p@ssw0rd"
# gvenzl image uses ORACLE_PASSWORD for SYS/SYSTEM/PDBADMIN.
_ROOT_PASSWORD = "rootpw"

# Oracle takes longer than Postgres/MySQL to come online: first-time
# database creation easily exceeds 60s. Allow ~3 minutes.
_READY_MAX_RETRIES = 90
_READY_BACKOFF_S = 3


@dataclass(frozen=True)
class DBConfig:

    host: str
    port: int
    user: str
    password: str
    database: str  # service name

    @property
    def uri(self) -> str:
        return (
            f"oracle+oracledb://{self.host}:{self.port}"
            f"/?service_name={self.database}"
        )

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        return (
            f"oracle+oracledb://{u}:{p}@{self.host}:{self.port}"
            f"/?service_name={self.database}"
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
            logger.info("pulling docker image %s", image)
            client.images.pull(image)
        except APIError as exc:
            _fail_or_skip(f"docker image {image!r} pull failed: {exc}")

    try:
        container = client.containers.run(
            image,
            name=name,
            environment=environment,
            ports={"1521/tcp": None},
            detach=True,
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
    bindings = ports.get("1521/tcp") or []
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


def _wait_for_db(host: str, port: int, user: str, password: str, db: str) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import DatabaseError

    u = urllib.parse.quote(user, safe="")
    p = urllib.parse.quote(password, safe="")
    url = f"oracle+oracledb://{u}:{p}@{host}:{port}/?service_name={db}"

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            engine = create_engine(url)
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1 FROM DUAL"))
                return
            finally:
                engine.dispose()
        except DatabaseError as e:
            # ``OperationalError`` is the *connect-failed* subclass;
            # ``gvenzl/oracle-free`` opens its TNS listener before
            # the FREEPDB1 PDB finishes mounting, and during that
            # window every query gets ``ORA-01109: database not
            # open``, which ``oracledb`` raises as a plain
            # ``DatabaseError``. Catching the parent covers both
            # cases.
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"Oracle at {host}:{port}/{db} did not accept connections "
        f"after {_READY_MAX_RETRIES} attempts (last error: {last_error})"
    )


def _seed_schema(cfg: DBConfig) -> None:
    from sqlalchemy import create_engine, text

    drop_if_exists = (
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tbl}'; "
        "EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    engine = create_engine(cfg.uri_with_credentials)
    try:
        with engine.begin() as conn:
            conn.execute(text(drop_if_exists.format(tbl="orders")))
            conn.execute(text(drop_if_exists.format(tbl="users")))
            conn.execute(text(
                "CREATE TABLE users ("
                "  id NUMBER(10) PRIMARY KEY,"
                "  name VARCHAR2(64) NOT NULL,"
                "  age NUMBER(10) NOT NULL"
                ")"
            ))
            conn.execute(text(
                "INSERT ALL "
                "INTO users (id, name, age) VALUES (1, 'alice', 30) "
                "INTO users (id, name, age) VALUES (2, 'bob', 25) "
                "INTO users (id, name, age) VALUES (3, 'carol', 40) "
                "SELECT * FROM DUAL"
            ))
            conn.execute(text(
                "CREATE TABLE orders ("
                "  id NUMBER(10) PRIMARY KEY,"
                "  user_id NUMBER(10) NOT NULL,"
                "  qty NUMBER(10) NOT NULL"
                ")"
            ))
            conn.execute(text(
                "INSERT ALL "
                "INTO orders (id, user_id, qty) VALUES (10, 1, 2) "
                "INTO orders (id, user_id, qty) VALUES (11, 2, 5) "
                "SELECT * FROM DUAL"
            ))
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

# Permissive host:port matcher: requires only the trailing slash,
# not a non-empty path component (Oracle URIs put the service name
# in the query string, not the path).
_HOST_PORT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://([^:/?#]+):(\d+)/")


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
            f"not a valid '<scheme>://host:port/...' URI. Unable to "
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
    *, name: str, image: str, environment: list[str],
) -> DBConfig:
    host_port = _ensure_container(
        name=name,
        image=image,
        environment=environment,
    )
    cfg = DBConfig(
        host="127.0.0.1",
        port=host_port,
        user=_DB_USER,
        password=_DB_PASSWORD,
        database=_DB_NAME,
    )
    _wait_for_db(cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)
    _seed_schema(cfg)
    return cfg


@pytest.fixture(scope="session")
def oracle_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_ORACLE_PREFIX,
        backend=_ORACLE_BACKEND,
        fixture="oracle_config",
    )
    if external is not None:
        if _should_reseed_external(_ORACLE_PREFIX, _ORACLE_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_ORACLE_CONTAINER,
            image=_ORACLE_IMAGE,
            environment=[
                f"ORACLE_PASSWORD={_ROOT_PASSWORD}",
                f"APP_USER={_DB_USER}",
                f"APP_USER_PASSWORD={_DB_PASSWORD}",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_oracle_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_ORACLE_CONTAINER,
                image=_ORACLE_IMAGE,
                environment=[
                    f"ORACLE_PASSWORD={_ROOT_PASSWORD}",
                    f"APP_USER={_DB_USER}",
                    f"APP_USER_PASSWORD={_DB_PASSWORD}",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_ORACLE_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``oracle_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_oracle(oracle_config, tmp_path_factory, worker_id) -> DBConfig:
    if worker_id == "master":
        _seed_schema(oracle_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_oracle_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(oracle_config)
    return oracle_config
