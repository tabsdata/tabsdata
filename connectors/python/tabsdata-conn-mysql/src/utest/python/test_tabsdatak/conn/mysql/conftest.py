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
from test_tabsdatak.conn.mysql.bootest import TESTING_RESOURCES_PATH


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


# --- Env var gates --------------------------------------------------

# Anything in TRUE_VALUES (case-insensitive) opts out of teardown.
TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"

# Default: failures starting docker / pulling images / running the
# container raise so CI surfaces the problem. Set this var truthy
# to convert those failures into ``pytest.skip`` -- useful on a
# laptop without docker.
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

# External-DB env-var prefixes. ``{prefix}__{backend}_ENVS`` truthy
# switches the fixture from docker mode to external mode.
_MYSQL_PREFIX = "MYSQL0"
_MYSQL_BACKEND = "MYSQL"
_MARIADB_PREFIX = "MARIADB0"
_MARIADB_BACKEND = "MARIADB"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

_MYSQL_IMAGE = "mysql:9.2.0"
_MARIADB_IMAGE = "mariadb:11.4.4"

_MYSQL_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "mysql"
)
_MARIADB_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "mariadb"
)

_DB_NAME = "testing"
_DB_USER = "etl"
_DB_PASSWORD = "p@ssw0rd"
_ROOT_PASSWORD = "rootpw"

_READY_MAX_RETRIES = 30
_READY_BACKOFF_S = 2


@dataclass(frozen=True)
class DBConfig:

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def uri(self) -> str:
        return f"mysql+pymysql://{self.host}:{self.port}/{self.database}"

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        return (
            f"mysql+pymysql://{u}:{p}@{self.host}:{self.port}/{self.database}"
        )

    @property
    def credentials(self) -> UserPassword:
        return UserPassword(
            user=Secret.lit(self.user),
            password=Secret.lit(self.password),
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
            ports={"3306/tcp": None},
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
    bindings = ports.get("3306/tcp") or []
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
    from sqlalchemy.exc import OperationalError

    u = urllib.parse.quote(user, safe="")
    p = urllib.parse.quote(password, safe="")
    url = f"mysql+pymysql://{u}:{p}@{host}:{port}/{db}"

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            engine = create_engine(url)
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return
            finally:
                engine.dispose()
        except OperationalError as e:
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"database at {host}:{port}/{db} did not accept connections "
        f"after {_READY_MAX_RETRIES} attempts (last error: {last_error})"
    )


def _seed_schema(cfg: DBConfig) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(cfg.uri_with_credentials)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS orders"))
            conn.execute(text("DROP TABLE IF EXISTS users"))
            conn.execute(text(
                "CREATE TABLE users ("
                "  id INT PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  age INT NOT NULL"
                ") ENGINE=InnoDB"
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
                ") ENGINE=InnoDB"
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


# --- External-DB env-var path ---------------------------------------

_HOST_PORT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://([^:/?#]+):(\d+)/[^?#]+")


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
def mysql_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_MYSQL_PREFIX, backend=_MYSQL_BACKEND, fixture="mysql_config",
    )
    if external is not None:
        if _should_reseed_external(_MYSQL_PREFIX, _MYSQL_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_MYSQL_CONTAINER,
            image=_MYSQL_IMAGE,
            environment=[
                f"MYSQL_ROOT_PASSWORD={_ROOT_PASSWORD}",
                f"MYSQL_DATABASE={_DB_NAME}",
                f"MYSQL_USER={_DB_USER}",
                f"MYSQL_PASSWORD={_DB_PASSWORD}",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_mysql_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_MYSQL_CONTAINER,
                image=_MYSQL_IMAGE,
                environment=[
                    f"MYSQL_ROOT_PASSWORD={_ROOT_PASSWORD}",
                    f"MYSQL_DATABASE={_DB_NAME}",
                    f"MYSQL_USER={_DB_USER}",
                    f"MYSQL_PASSWORD={_DB_PASSWORD}",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_MYSQL_CONTAINER)


@pytest.fixture(scope="session")
def mariadb_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_MARIADB_PREFIX, backend=_MARIADB_BACKEND,
        fixture="mariadb_config",
    )
    if external is not None:
        if _should_reseed_external(_MARIADB_PREFIX, _MARIADB_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_MARIADB_CONTAINER,
            image=_MARIADB_IMAGE,
            environment=[
                f"MARIADB_ROOT_PASSWORD={_ROOT_PASSWORD}",
                f"MARIADB_DATABASE={_DB_NAME}",
                f"MARIADB_USER={_DB_USER}",
                f"MARIADB_PASSWORD={_DB_PASSWORD}",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_mariadb_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_MARIADB_CONTAINER,
                image=_MARIADB_IMAGE,
                environment=[
                    f"MARIADB_ROOT_PASSWORD={_ROOT_PASSWORD}",
                    f"MARIADB_DATABASE={_DB_NAME}",
                    f"MARIADB_USER={_DB_USER}",
                    f"MARIADB_PASSWORD={_DB_PASSWORD}",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_MARIADB_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``mysql_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_mysql(mysql_config, tmp_path_factory, worker_id) -> DBConfig:
    if worker_id == "master":
        _seed_schema(mysql_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_mysql_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(mysql_config)
    return mysql_config
