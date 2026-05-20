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
from test_tabsdatak.conn.postgres.bootest import TESTING_RESOURCES_PATH


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

TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

_POSTGRES_PREFIX = "POSTGRES0"
_POSTGRES_BACKEND = "POSTGRES"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

_POSTGRES_IMAGE = "postgres:17.2"
_POSTGRES_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "postgres"
)

_DB_NAME = "testing"
_DB_USER = "etl"
_DB_PASSWORD = "p@ssw0rd"

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
        return f"postgresql+psycopg2://{self.host}:{self.port}/{self.database}"

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        return (
            f"postgresql+psycopg2://{u}:{p}@{self.host}:{self.port}/{self.database}"
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
        existing.reload()
        host_port = _container_port(existing)
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
            ports={"5432/tcp": None},
            detach=True,
        )
    except APIError as exc:
        _fail_or_skip(f"docker run {image!r} failed: {exc}")

    container.reload()
    host_port = _container_port(container)
    if host_port is None:
        container.remove(force=True)
        _fail_or_skip("docker container started but published no host port")
    return host_port


def _container_port(container) -> int | None:
    ports = (container.attrs.get("NetworkSettings") or {}).get("Ports") or {}
    bindings = ports.get("5432/tcp") or []
    if not bindings:
        return None
    try:
        return int(bindings[0]["HostPort"])
    except (KeyError, ValueError, IndexError):
        return None


def _wait_for_db(host: str, port: int, user: str, password: str, db: str) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import OperationalError

    u = urllib.parse.quote(user, safe="")
    p = urllib.parse.quote(password, safe="")
    url = f"postgresql+psycopg2://{u}:{p}@{host}:{port}/{db}"

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
                "  id INTEGER PRIMARY KEY,"
                "  name VARCHAR(64) NOT NULL,"
                "  age INTEGER NOT NULL"
                ")"
            ))
            conn.execute(text(
                "INSERT INTO users (id, name, age) VALUES "
                "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"
            ))
            conn.execute(text(
                "CREATE TABLE orders ("
                "  id INTEGER PRIMARY KEY,"
                "  user_id INTEGER NOT NULL,"
                "  qty INTEGER NOT NULL"
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
def postgres_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_POSTGRES_PREFIX,
        backend=_POSTGRES_BACKEND,
        fixture="postgres_config",
    )
    if external is not None:
        if _should_reseed_external(_POSTGRES_PREFIX, _POSTGRES_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_POSTGRES_CONTAINER,
            image=_POSTGRES_IMAGE,
            environment=[
                f"POSTGRES_DB={_DB_NAME}",
                f"POSTGRES_USER={_DB_USER}",
                f"POSTGRES_PASSWORD={_DB_PASSWORD}",
            ],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_postgres_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_POSTGRES_CONTAINER,
                image=_POSTGRES_IMAGE,
                environment=[
                    f"POSTGRES_DB={_DB_NAME}",
                    f"POSTGRES_USER={_DB_USER}",
                    f"POSTGRES_PASSWORD={_DB_PASSWORD}",
                ],
            )
    try:
        yield cfg
    finally:
        if worker_id == "master":
            _remove_container(_POSTGRES_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``postgres_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_postgres(postgres_config, tmp_path_factory, worker_id) -> DBConfig:
    if worker_id == "master":
        _seed_schema(postgres_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_postgres_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(postgres_config)
    return postgres_config
