#
# Copyright 2026 Tabsdata Inc.
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
from tenacity import retry, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdatak.conn.starrocks.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from test_tabsdatak._testing import setup_tests_logging
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    UserPassword,
)
from tabsdatak.conn.starrocks import S3Location
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
        return
    clean_everything()


# --- Secret gates --------------------------------------------------

TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

_STARROCKS_PREFIX = "STARROCKS0"
_STARROCKS_BACKEND = "STARROCKS"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> "None":
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


# --- Constants ------------------------------------------------------

# starrocks/allin1-ubuntu bundles FE + BE in one container; the MySQL
# protocol port is 9030.
_STARROCKS_IMAGE = "starrocks/allin1-ubuntu:3.3-latest"
_STARROCKS_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "starrocks"
)

# StarRocks ``root`` user requires no password by default.
_DB_NAME = "testing"
_DB_USER = "root"
_DB_PASSWORD = ""

# StarRocks needs longer than Postgres / MySQL to come up: FE bootstrap
# + BE registration is on the order of 60s on a cold start.
_READY_MAX_RETRIES = 90
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
        return f"starrocks://{self.host}:{self.port}/{self.database}"

    @property
    def uri_with_credentials(self) -> str:
        u = urllib.parse.quote(self.user, safe="")
        p = urllib.parse.quote(self.password, safe="")
        return (
            f"mysql+mysqlconnector://{u}:{p}@{self.host}:{self.port}/{self.database}"
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
        # StarRocks also exposes ``8030`` (FE HTTP), ``8040`` (BE HTTP)
        # and ``9050`` (RPC); we only need the MySQL protocol port for
        # the tests, so ``9030`` is the only one we forward.
        container = client.containers.run(
            image,
            name=name,
            environment=environment,
            ports={"9030/tcp": None},
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
    bindings = ports.get("9030/tcp") or []
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
    # Initial probe goes against the default ``information_schema`` so
    # we don't depend on the target db being created yet.
    url_probe = f"mysql+mysqlconnector://{u}:{p}@{host}:{port}/information_schema"

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            engine = create_engine(url_probe)
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                # Ensure the target database exists.
                with engine.begin() as conn:
                    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db}`"))
                # The FE accepts queries before the BE registers; if
                # we proceed straight to CREATE TABLE we get a
                # "Cluster has no available capacity" failure. Block
                # until at least one BE row shows Alive=true.
                _wait_for_backend_alive(engine)
                with engine.begin() as conn:
                    conn.execute(text(
                        'admin set frontend config '
                        '("storage_usage_hard_limit_reserve_bytes" = "1073741824")'
                    ))
                return
            finally:
                engine.dispose()
        except (OperationalError, InterfaceError) as e:
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"starrocks at {host}:{port}/{db} did not accept connections "
        f"after {_READY_MAX_RETRIES} attempts (last error: {last_error})"
    )


def _wait_for_backend_alive(engine) -> None:
    """Poll ``SHOW BACKENDS`` until at least one BE reports Alive=true.

    The allin1-ubuntu image starts the FE before its bundled BE has
    finished registering; queries that need storage capacity (CREATE
    TABLE, INSERT) fail with ``Cluster has no available capacity``
    until the BE is in the cluster.
    """
    from sqlalchemy import text

    deadline = time.monotonic() + _BE_READY_TIMEOUT_S
    last_status = "no BEs registered"
    while time.monotonic() < deadline:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW BACKENDS")).mappings().all()
            alive = [r for r in rows if str(r.get("Alive")).lower() == "true"]
            if alive:
                logger.info(
                    "StarRocks BE registered: %s alive of %s",
                    len(alive), len(rows),
                )
                return
            last_status = f"{len(rows)} BE(s) registered, none Alive yet"
        except Exception as exc:  # noqa: BLE001
            last_status = f"SHOW BACKENDS failed: {exc!r}"
        time.sleep(2.0)
    _fail_or_skip(
        f"StarRocks BE not alive after {_BE_READY_TIMEOUT_S}s "
        f"(last status: {last_status})"
    )


_BE_READY_TIMEOUT_S = 180

# Mirror the production-side StarRocks retry policy from
# ``tabsdata/_io/cloud_staging/starrocks/dialect.py``: StarRocks
# transiently reports "Current available backends: []" under heavy
# concurrent transactional load (single-BE container on emulated
# ARM). Retry the seed for up to 5 minutes on a 5-second cadence.
_STARROCKS_QUERY_RETRY_TIMEOUT_SECONDS = 300
_STARROCKS_QUERY_RETRY_INTERVAL_SECONDS = 5


@retry(
    stop=stop_after_delay(_STARROCKS_QUERY_RETRY_TIMEOUT_SECONDS),
    wait=wait_fixed(_STARROCKS_QUERY_RETRY_INTERVAL_SECONDS),
    reraise=True,
)
def _seed_schema(cfg: DBConfig) -> None:
    """Drop + recreate the canonical ``users`` / ``orders`` tables that
    the integration tests load into. We do *not* seed rows -- the
    plugin tests insert from parquet via FILES() and verify the result.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(cfg.uri_with_credentials)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS orders"))
            conn.execute(text("DROP TABLE IF EXISTS users"))
            conn.execute(text(
                "CREATE TABLE users ("
                "  id INT NOT NULL,"
                "  name STRING NOT NULL,"
                "  age INT NOT NULL"
                ") DUPLICATE KEY(id) "
                "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                "PROPERTIES (\"replication_num\" = \"1\")"
            ))
            conn.execute(text(
                "INSERT INTO users (id, name, age) VALUES "
                "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"
            ))
            conn.execute(text(
                "CREATE TABLE orders ("
                "  id INT NOT NULL,"
                "  user_id INT NOT NULL,"
                "  qty INT NOT NULL"
                ") DUPLICATE KEY(id) "
                "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                "PROPERTIES (\"replication_num\" = \"1\")"
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
    password = os.environ.get(f"{prefix}__{backend}_PASSWORD", "")
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
def starrocks_config(tmp_path_factory, worker_id) -> Iterator[DBConfig]:
    external = _external_config(
        prefix=_STARROCKS_PREFIX,
        backend=_STARROCKS_BACKEND,
        fixture="starrocks_config",
    )
    if external is not None:
        if _should_reseed_external(_STARROCKS_PREFIX, _STARROCKS_BACKEND):
            _seed_schema(external)
        yield external
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_STARROCKS_CONTAINER,
            image=_STARROCKS_IMAGE,
            environment=[],
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_starrocks_config_creation.lock"
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_STARROCKS_CONTAINER,
                image=_STARROCKS_IMAGE,
                environment=[],
            )
    try:
        yield cfg
    finally:
        _remove_container(_STARROCKS_CONTAINER)


# Per-test re-seed so write tests don't leak rows into subsequent
# reader tests. The session-scoped ``starrocks_config`` keeps the
# container itself alive.
@pytest.fixture
def fresh_starrocks(
    starrocks_config, tmp_path_factory, worker_id,
) -> DBConfig:
    if worker_id == "master":
        _seed_schema(starrocks_config)
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = root_tmp_dir / "docker_starrocks_config_seed.lock"
        with FileLock(str(lock_path)):
            _seed_schema(starrocks_config)
    return starrocks_config


# --- S3 staging fixture ---------------------------------------------
#
# StarRocks bulk loads require cloud staging. We expose ``s3_staging``
# as the canonical fixture for the integration tests; it is built from
# the shared ``S30__S3_*`` var bundle (same one the other S3-backed
# integrations consume) and *skips* when those aren't set rather than
# hard-failing -- the docker container alone isn't enough to exercise
# the full plugin path.


_S3_PREFIX = "S30"
# Optional StarRocks-specific override for the base path inside the
# bucket. Keeps the StarRocks integration writes isolated from the
# generic S3 staging used by sibling connectors.
_S3_BASE_PATH_ENV = "STARROCKS0__S3_STAGING_BASE_PATH"


def _split_s3_uri(uri: str) -> tuple[str, str | None]:
    """Split ``s3://<bucket>[/<path>]`` into ``(bucket, base_path)``.

    Returns ``base_path=None`` when the URI carries only a bucket so
    callers can apply their own default.
    """
    prefix = "s3://"
    if not uri.startswith(prefix):
        raise ValueError(
            f"{_S3_PREFIX}__S3_URI must start with s3://: {uri!r}"
        )
    rest = uri[len(prefix):].strip("/")
    if "/" in rest:
        bucket, base_path = rest.split("/", 1)
        return bucket, "/" + base_path.strip("/")
    return rest, None


@pytest.fixture(scope="session")
def s3_staging() -> S3Location:
    uri = os.environ.get(f"{_S3_PREFIX}__S3_URI")
    region = os.environ.get(f"{_S3_PREFIX}__S3_REGION")
    access_key = os.environ.get(f"{_S3_PREFIX}__S3_ACCESS_KEY")
    secret_key = os.environ.get(f"{_S3_PREFIX}__S3_SECRET_KEY")
    if not (uri and region and access_key and secret_key):
        pytest.skip(
            "S3 staging not configured. Set "
            f"{_S3_PREFIX}__S3_URI / {_S3_PREFIX}__S3_REGION / "
            f"{_S3_PREFIX}__S3_ACCESS_KEY / {_S3_PREFIX}__S3_SECRET_KEY "
            "to run the StarRocks integration tests."
        )
    bucket, uri_base_path = _split_s3_uri(uri)
    base_path = os.environ.get(
        _S3_BASE_PATH_ENV,
        uri_base_path if uri_base_path is not None else "/tabsdata-starrocks-integ",
    )
    return S3Location(
        bucket=bucket,
        region=region,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(access_key),
            secret_access_key=Secret(secret_key),
        ),
        base_path=base_path,
    )
