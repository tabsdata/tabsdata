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
from test_tabsdatak.conn.mssql.bootest import TESTING_RESOURCES_PATH


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
# laptop without docker or without the Microsoft ODBC driver.
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

# External-DB var prefix. ``{prefix}__{backend}_ENVS`` truthy
# switches the fixture from docker mode to external mode. The
# (typo'd) ``MSSSQL0`` prefix is kept verbatim for cross-repo
# compatibility with tabsdata-os.
_MSSQL_PREFIX = "MSSSQL0"
_MSSQL_BACKEND = "MSSQL"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

_MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
_MSSQL_PLATFORM = "linux/amd64"
_MSSQL_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "mssql"
)

_DB_NAME = "testing"
_DB_USER = "sa"
# SQL Server enforces password complexity: 8+ chars with three of
# {upper, lower, digit, symbol}. This satisfies all four classes.
_DB_PASSWORD = "Tabsdata-pw_2026"

# SQL Server normally boots in 30-60s.
_READY_MAX_RETRIES = 30
_READY_BACKOFF_S = 3

_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


@dataclass(frozen=True)
class DBConfig:

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def uri(self) -> str:
        # Driver-qualified URI with the ODBC driver query parameter
        # and TrustServerCertificate=yes so the developer image
        # (self-signed TLS cert) works without further setup.
        driver = urllib.parse.quote_plus(_ODBC_DRIVER)
        return (
            f"mssql+pyodbc://{self.host}:{self.port}/{self.database}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        driver = urllib.parse.quote_plus(_ODBC_DRIVER)
        return (
            f"mssql+pyodbc://{u}:{p}@{self.host}:{self.port}/{self.database}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )

    @property
    def odbc_connection_string(self) -> str:
        # Raw ODBC connection string (no ``://`` scheme). Credentials are
        # deliberately NOT embedded -- the explorer supplies UID / PWD via
        # connect_args from the conn's ``credentials``.
        return (
            f"DRIVER={{{_ODBC_DRIVER}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            "TrustServerCertificate=yes;"
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


def _require_pyodbc_driver() -> None:
    try:
        import pyodbc
    except ImportError:
        _fail_or_skip("pyodbc not installed (`pip install pyodbc`)")
        return
    if _ODBC_DRIVER not in pyodbc.drivers():
        _fail_or_skip(
            f"{_ODBC_DRIVER!r} ODBC driver not found on host; "
            "install Microsoft ODBC driver 18 to run mssql plugin tests"
        )


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
            client.images.pull(image, platform=_MSSQL_PLATFORM)
        except APIError as exc:
            _fail_or_skip(f"docker image {image!r} pull failed: {exc}")

    try:
        container = client.containers.run(
            image,
            name=name,
            environment=environment,
            ports={"1433/tcp": None},
            detach=True,
        platform=_MSSQL_PLATFORM,
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
    bindings = ports.get("1433/tcp") or []
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
    from sqlalchemy.exc import InterfaceError, OperationalError

    u = urllib.parse.quote(user, safe="")
    p = urllib.parse.quote(password, safe="")
    driver = urllib.parse.quote_plus(_ODBC_DRIVER)
    master_url = (
        f"mssql+pyodbc://{u}:{p}@{host}:{port}/master"
        f"?driver={driver}&TrustServerCertificate=yes"
    )

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            engine = create_engine(master_url)
            try:
                # SQL Server's CREATE DATABASE can't run in a
                # transaction; use an autocommit connection.
                with engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT",
                ) as conn:
                    conn.execute(text("SELECT 1"))
                    exists = conn.execute(
                        text(
                            "SELECT 1 FROM sys.databases WHERE name = :name"
                        ),
                        {"name": db},
                    ).scalar()
                    if not exists:
                        conn.execute(text(f"CREATE DATABASE [{db}]"))
                return
            finally:
                engine.dispose()
        except (InterfaceError, OperationalError) as e:
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"SQL Server at {host}:{port} did not accept connections "
        f"after {_READY_MAX_RETRIES} attempts (last error: {last_error})"
    )


def _seed_schema(cfg: DBConfig) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(cfg.uri_with_credentials)
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "IF OBJECT_ID('orders', 'U') IS NOT NULL DROP TABLE orders"
            ))
            conn.execute(text(
                "IF OBJECT_ID('users', 'U') IS NOT NULL DROP TABLE users"
            ))
            conn.execute(text(
                "CREATE TABLE users ("
                "  id INT PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  age INT NOT NULL"
                ")"
            ))
            conn.execute(text(
                "INSERT INTO users (id, name, age) VALUES "
                "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"
            ))
            conn.execute(text(
                "CREATE TABLE orders ("
                "  id INT PRIMARY KEY,"
                "  user_id INT NOT NULL,"
                "  qty INT NOT NULL"
                ")"
            ))
            conn.execute(text(
                "INSERT INTO orders (id, user_id, qty) VALUES "
                "(10, 1, 2), (11, 2, 5)"
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

_HOST_PORT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+._-]*://([^:/?#]+):(\d+)/")


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
def mssql_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    _require_pyodbc_driver()

    external = _external_config(
        prefix=_MSSQL_PREFIX, backend=_MSSQL_BACKEND, fixture="mssql_config",
    )
    if external is not None:
        if _should_reseed_external(_MSSQL_PREFIX, _MSSQL_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_MSSQL_CONTAINER,
            image=_MSSQL_IMAGE,
            environment=[
                "ACCEPT_EULA=Y",
                f"MSSQL_SA_PASSWORD={_DB_PASSWORD}",
                "MSSQL_PID=Developer",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_mssql_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_MSSQL_CONTAINER,
                image=_MSSQL_IMAGE,
                environment=[
                    "ACCEPT_EULA=Y",
                    f"MSSQL_SA_PASSWORD={_DB_PASSWORD}",
                    "MSSQL_PID=Developer",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_MSSQL_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``mssql_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_mssql(mssql_config, tmp_path_factory, worker_id) -> DBConfig:
    if worker_id == "master":
        _seed_schema(mssql_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_mssql_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(mssql_config)
    return mssql_config
