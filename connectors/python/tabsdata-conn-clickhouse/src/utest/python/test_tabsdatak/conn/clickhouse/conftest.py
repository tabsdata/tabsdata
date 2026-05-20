#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import dataclasses
import datetime
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Iterator

from filelock import FileLock

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdatak.conn.clickhouse.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    CloudLocation,
    GCSLocation,
    S3Location,
)
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
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        return
    clean_everything()


# --- Env var gates ---------------------------------------------------

TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
TD_DOCKER_DONT_KILL = "TD_DOCKER_DONT_KILL"

# Default: docker / pull / run failures raise so CI surfaces them.
# Truthy => convert to ``pytest.skip`` (useful on a laptop without
# docker).
SKIP_ON_DOCKER_FAIL = "SKIP_ON_DOCKER_FAIL"

# ``{prefix}__{backend}_ENVS`` truthy => skip docker, use external.
_CLICKHOUSE_PREFIX = "CLICKHOUSE0"
_CLICKHOUSE_BACKEND = "CLICKHOUSE"


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_VALUES


def _dont_kill() -> bool:
    return _truthy(TD_DOCKER_DONT_KILL)


def _fail_or_skip(reason: str) -> None:
    if _truthy(SKIP_ON_DOCKER_FAIL):
        pytest.skip(reason)
    raise RuntimeError(reason)


def _maybe_env(name: str) -> str | None:
    val = os.environ.get(name)
    return val if val else None


def _required_env(name: str, fixture: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"environment variable {name} is not set. Unable to run"
            f" tests using the {fixture!r} fixture in external"
            f" mode."
        )
    return val


# --- Constants -------------------------------------------------------

_CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.8"
_CLICKHOUSE_CONTAINER = (
    f"{PYTEST_DEFAULT_ENVIRONMENT_PREFIX}"
    f"{DOCKER_UTEST_CONTAINER_INFIX}"
    f"{DOCKER_REGULAR_CONTAINER_INFIX}"
    "clickhouse"
)
_CLICKHOUSE_HTTP_PORT_INSIDE = "8123/tcp"

_DB_NAME = "default"
_DB_USER = "default"
_DB_PASSWORD = "tabsdata"

_READY_MAX_RETRIES = 30
_READY_BACKOFF_S = 2


# --- Config dataclass -----------------------------------------------


@dataclass(frozen=True)
class CHConfig:
    """ClickHouse server + staging config consumed by the
    integration tests.

    The ``credentials`` / ``staging`` properties produce new-model
    (``tabsdatak.conn.common.types``) values, so an integration
    test can build a ``ClickHouseConn`` straight from the fields::

        ClickHouseConn(
            host=cfg.host, port=cfg.port, database=cfg.database,
            credentials=cfg.credentials, staging=cfg.staging,
            secure=cfg.secure,
        )
    """

    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool
    staging: CloudLocation

    @property
    def credentials(self) -> UserPassword:
        return UserPassword(
            user=Secret.lit(self.user),
            password=Secret.lit(self.password),
        )


# --- Docker helpers --------------------------------------------------


def _require_docker():
    try:
        import docker
    except ImportError:
        _fail_or_skip(
            "docker SDK not installed (`pip install docker`)"
        )
        return
    try:
        client = docker.from_env()
        client.ping()
    except Exception as exc:  # noqa: BLE001
        _fail_or_skip(f"docker daemon unreachable: {exc}")
    return client


def _container_port(container) -> int | None:
    ports = (
        container.attrs.get("NetworkSettings") or {}
    ).get("Ports") or {}
    bindings = ports.get(_CLICKHOUSE_HTTP_PORT_INSIDE) or []
    if not bindings:
        return None
    try:
        return int(bindings[0]["HostPort"])
    except (KeyError, ValueError, IndexError):
        return None


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
            _fail_or_skip(
                f"docker image {image!r} pull failed: {exc}"
            )

    try:
        container = client.containers.run(
            image,
            name=name,
            environment=environment,
            ports={_CLICKHOUSE_HTTP_PORT_INSIDE: None},
            ulimits=[
                # ClickHouse recommends a high nofile limit.
                {"Name": "nofile", "Soft": 262144, "Hard": 262144},
            ],
            detach=True,
        )
    except APIError as exc:
        _fail_or_skip(
            f"docker run {image!r} failed: {exc}"
        )

    container.reload()
    host_port = _container_port(container)
    if host_port is None:
        container.remove(force=True)
        _fail_or_skip(
            "docker container started but published no host port"
        )
    return host_port


def _wait_for_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    secure: bool,
) -> None:
    import clickhouse_connect

    last_error: Exception | None = None
    for attempt in range(1, _READY_MAX_RETRIES + 1):
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                database=db,
                username=user,
                password=password,
                secure=secure,
            )
            client.query("SELECT 1")
            return
        except Exception as e:  # noqa: BLE001 -- readiness loop
            last_error = e
            time.sleep(min(_READY_BACKOFF_S * attempt, 10))
    _fail_or_skip(
        f"clickhouse at {host}:{port}/{db} did not accept"
        f" connections after {_READY_MAX_RETRIES} attempts"
        f" (last error: {last_error})"
    )


def _remove_container(name: str) -> None:
    if _dont_kill():
        logger.info(
            "TD_DOCKER_DONT_KILL is set; leaving container %r"
            " running",
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
    except Exception as exc:  # noqa: BLE001 -- teardown
        logger.warning(
            "failed to remove container %r (safe to ignore on a"
            " docker-less machine): %s",
            name,
            exc,
        )


# --- External-mode config -------------------------------------------


def _external_server_config(
    *, prefix: str, backend: str, fixture: str,
) -> tuple[str, int, str, str, str, bool] | None:
    """Read the (host, port, user, password, database, secure) tuple
    from env vars if external mode is enabled; return ``None`` if not.
    """
    if not _truthy(f"{prefix}__{backend}_ENVS"):
        return None
    host = _required_env(f"{prefix}__{backend}_HOST", fixture)
    port = int(_required_env(f"{prefix}__{backend}_PORT", fixture))
    user = _required_env(f"{prefix}__{backend}_USER", fixture)
    password = _required_env(
        f"{prefix}__{backend}_PASSWORD", fixture
    )
    database = _required_env(
        f"{prefix}__{backend}_DATABASE", fixture
    )
    secure = _truthy(f"{prefix}__{backend}_SECURE")
    return host, port, user, password, database, secure


# --- Staging (cloud location) ---------------------------------------
#
# The ClickHouse dest plugin uses a cloud-staged bulk loader: parquet
# files are uploaded to S3 / Azure / GCS, then ClickHouse ingests them
# via ``s3()`` / ``azureBlobStorage()`` table functions. Integration
# tests therefore need both a real server *and* a real cloud staging
# bucket.
#
# Staging env vars follow the conventions already used by the other
# integration suites (``tabsdata-conn-s3``, ``tabsdata-conn-azureblob``,
# ``tabsdata-conn-gcs``):
#
#   S3:    ``S30__S3_URI`` (``s3://bucket[/prefix]``), ``S30__S3_REGION``,
#          ``S30__S3_ACCESS_KEY``, ``S30__S3_SECRET_KEY``
#   Azure: ``AZ0__AZ_URI`` (``az://container[/prefix]``),
#          ``AZ0__AZ_ACCOUNT_NAME``, ``AZ0__AZ_ACCOUNT_KEY``
#   GCS:   ``GCP0__GCP_STORAGE_URI`` (``gs://bucket[/prefix]``),
#          ``GCP0__GCP_SERVICE_ACCOUNT_KEY``
#
# ``CLICKHOUSE0__STAGING_PROVIDER`` (one of ``s3`` / ``azure`` /
# ``gcs``) selects which family is consulted; default is ``s3``.
# Absent or empty credentials in the chosen provider's variables
# means "skip integration tests" rather than "fail".


def _parse_uri(uri: str) -> str:
    """Return the ``netloc`` (bucket / container) of a
    ``<scheme>://bucket[/path]`` URI.

    The path component is intentionally discarded -- integration
    tests use a fresh, session-scoped timestamp prefix instead (see
    :data:`_SESSION_BASE_PATH`) so each test run writes under its
    own isolated subtree and prior runs' cruft can't affect the
    cleanup sweep.
    """
    from urllib.parse import urlparse

    return urlparse(uri).netloc


# Session-scoped staging prefix: ``YYYYMMDDTHHMMSS`` of the moment
# conftest is loaded. Same value across every test in the session;
# different value across runs.
_SESSION_BASE_PATH = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")


def _staging_config(prefix: str) -> CloudLocation | None:
    provider = (
        os.environ.get(f"{prefix}__STAGING_PROVIDER", "s3")
        .strip()
        .lower()
    )
    if provider == "s3":
        uri = _maybe_env("S30__S3_URI")
        region = _maybe_env("S30__S3_REGION")
        access = _maybe_env("S30__S3_ACCESS_KEY")
        secret = _maybe_env("S30__S3_SECRET_KEY")
        if not (uri and region and access and secret):
            return None
        return S3Location(
            bucket=Secret.lit(_parse_uri(uri)),
            region=Secret.lit(region),
            base_path=_SESSION_BASE_PATH,
            credentials=AwsAccessSecretKey(
                access_key_id=Secret.lit(access),
                secret_access_key=Secret.lit(secret),
            ),
        )
    if provider == "azure":
        uri = _maybe_env("AZ0__AZ_URI")
        account_name = _maybe_env("AZ0__AZ_ACCOUNT_NAME")
        account_key = _maybe_env("AZ0__AZ_ACCOUNT_KEY")
        if not (uri and account_name and account_key):
            return None
        return AzBlobLocation(
            container=Secret.lit(_parse_uri(uri)),
            base_path=_SESSION_BASE_PATH,
            credentials=AzureAccountKey(
                account_name=Secret.lit(account_name),
                account_key=Secret.lit(account_key),
            ),
        )
    if provider == "gcs":
        uri = _maybe_env("GCP0__GCP_STORAGE_URI")
        sa_json = _maybe_env("GCP0__GCP_SERVICE_ACCOUNT_KEY")
        hmac_access = _maybe_env("GCP0__GCP_STORAGE_ACCESS_KEY")
        hmac_secret = _maybe_env("GCP0__GCP_STORAGE_SECRET_KEY")
        if not (uri and sa_json and hmac_access and hmac_secret):
            return None
        # The clickhouse-local GCSLocation requires both credentials:
        #   - GCPJson (service-account JSON) drives the native GCS
        #     SDK upload / list / delete (fast path).
        #   - GCPHmac drives ClickHouse's ``s3()`` table function on
        #     the read side (the only protocol ClickHouse speaks to
        #     GCS).
        return GCSLocation(
            bucket=Secret.lit(_parse_uri(uri)),
            base_path=_SESSION_BASE_PATH,
            credentials=GCPJson(json=Secret.lit(sa_json)),
            hmac_credentials=GCPHmac(
                access_key=Secret.lit(hmac_access),
                secret_key=Secret.lit(hmac_secret),
            ),
        )
    raise RuntimeError(
        f"unknown staging provider {provider!r}: expected"
        f" 's3', 'azure', or 'gcs' (env"
        f" {prefix}__STAGING_PROVIDER)"
    )


# --- Public fixtures ------------------------------------------------


def _build_config(
    *,
    name: str,
    image: str,
    staging: CloudLocation,
) -> CHConfig:
    host_port = _ensure_container(
        name=name,
        image=image,
        environment=[
            f"CLICKHOUSE_PASSWORD={_DB_PASSWORD}",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
        ],
    )
    cfg = CHConfig(
        host="127.0.0.1",
        port=host_port,
        user=_DB_USER,
        password=_DB_PASSWORD,
        database=_DB_NAME,
        secure=False,
        staging=staging,
    )
    _wait_for_clickhouse(
        cfg.host, cfg.port, cfg.user, cfg.password,
        cfg.database, cfg.secure,
    )
    return cfg


@pytest.fixture(scope="session")
def clickhouse_config(
    tmp_path_factory, worker_id,
) -> Iterator[CHConfig]:
    """Bring up (or attach to) a ClickHouse server and pair it with
    a cloud-staging location.

    External mode (``CLICKHOUSE0__CLICKHOUSE_ENVS`` truthy): read
    server params from ``CLICKHOUSE0__CLICKHOUSE_{HOST,PORT,USER,
    PASSWORD,DATABASE,SECURE}``.

    Otherwise: spin up the official ClickHouse docker image, expose
    the HTTP port on a free host port, wait for readiness, and tear
    the container down at session end (unless
    ``TD_DOCKER_DONT_KILL`` is set).

    Staging comes from ``CLICKHOUSE0__STAGING_*`` env vars (see
    :func:`_staging_config`). If no staging is configured the
    fixture skips every test that asks for it, since the dest plugin
    requires a cloud-staging location to function.
    """
    staging = _staging_config(_CLICKHOUSE_PREFIX)
    if staging is None:
        pytest.skip(
            "ClickHouse integration tests need a cloud-staging"
            " location. By default the suite reads S3 envs"
            " (S30__S3_URI + S30__S3_REGION + S30__S3_ACCESS_KEY +"
            " S30__S3_SECRET_KEY); set"
            f" {_CLICKHOUSE_PREFIX}__STAGING_PROVIDER=azure or"
            " =gcs to use AZ0__/GCP0__ env vars instead."
        )

    server = _external_server_config(
        prefix=_CLICKHOUSE_PREFIX,
        backend=_CLICKHOUSE_BACKEND,
        fixture="clickhouse_config",
    )
    if server is not None:
        host, port, user, password, database, secure = server
        yield CHConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            secure=secure,
            staging=staging,
        )
        return

    if worker_id == "master":
        cfg = _build_config(
            name=_CLICKHOUSE_CONTAINER,
            image=_CLICKHOUSE_IMAGE,
            staging=staging,
        )
    else:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = (
            root_tmp_dir / "docker_clickhouse_config_creation.lock"
        )
        with FileLock(str(lock_path)):
            cfg = _build_config(
                name=_CLICKHOUSE_CONTAINER,
                image=_CLICKHOUSE_IMAGE,
                staging=staging,
            )
    try:
        yield cfg
    finally:
        _remove_container(_CLICKHOUSE_CONTAINER)


_TEST_NAME_SAFE_RE = re.compile(r"[^A-Za-z0-9_]+")


def _sanitize_for_path(name: str) -> str:
    """Reduce ``name`` to ``[A-Za-z0-9_]+`` so it's a safe single
    cloud-storage path segment. ``test_fqn_create_table[bare-table]``
    becomes ``test_fqn_create_table_bare_table``.
    """
    return _TEST_NAME_SAFE_RE.sub("_", name).strip("_")


def _extend_base_path(
    location: CloudLocation, extra: str
) -> CloudLocation:
    """Return a copy of ``location`` with ``base_path/extra`` as the
    new base_path. Hand-coded per subclass because the conn-clickhouse
    cloud locations are pydantic-flavored dataclasses (kw_only,
    frozen-ish) without a stable shallow-copy helper.
    """
    base = (location.base_path or "").rstrip("/")
    new_base_path = f"{base}/{extra}" if base else extra

    if isinstance(location, S3Location):
        return S3Location(
            bucket=location.bucket,
            region=location.region,
            credentials=location.credentials,
            base_path=new_base_path,
            cfg=location.cfg,
        )
    if isinstance(location, AzBlobLocation):
        return AzBlobLocation(
            container=location.container,
            credentials=location.credentials,
            base_path=new_base_path,
            cfg=location.cfg,
        )
    if isinstance(location, GCSLocation):
        return GCSLocation(
            bucket=location.bucket,
            credentials=location.credentials,
            hmac_credentials=location.hmac_credentials,
            base_path=new_base_path,
            cfg=location.cfg,
        )
    raise TypeError(
        f"Unsupported staging location type: {type(location).__name__}"
    )


@pytest.fixture
def fresh_clickhouse(clickhouse_config, request) -> CHConfig:
    """Per-test entry point: returns a :class:`CHConfig` whose
    ``staging`` location has its ``base_path`` extended with the
    current test's sanitized name.

    The full cloud-storage layout per run is::

        <bucket-or-container>/<SESSION_BASE_PATH>/<test_name>/<upload_id>/<file>

    where ``SESSION_BASE_PATH`` (``YYYYMMDDTHHMMSS`` set at conftest
    load) is identical across every test in the session, and the
    per-test segment isolates each test's files into its own subtree.
    That means:

    - Two tests cannot trip each other's cleanup sweep (each
      ``delete(up_to_id)`` walks only the test's own subtree).
    - Two sessions cannot trip each other (different timestamp roots).
    - The bucket can be inspected after a failed run to see exactly
      which test left orphans, by reading the per-test segment.

    Per-test table cleanup (the database-side counterpart) is handled
    by the test-level ``fqn_table`` fixture below.
    """
    test_segment = _sanitize_for_path(request.node.name)
    new_staging = _extend_base_path(
        clickhouse_config.staging, test_segment
    )
    return dataclasses.replace(clickhouse_config, staging=new_staging)


# --- Per-test scratch dir + per-test ClickHouse table ---------------


@pytest.fixture
def work_dir(tmp_path):
    """Per-test scratch directory under pytest's ``tmp_path``."""
    from pathlib import Path

    out = Path(str(tmp_path)) / "work"
    out.mkdir()
    return out


@pytest.fixture
def fqn_table(fresh_clickhouse, request):
    """Allocate a unique scratch table per test (or per parametrize
    case) and drop it before and after.

    Yields ``(db, tbl)``. The table name encodes the test's original
    name plus its parametrize id (when present) so concurrent /
    sequential cases never collide on the same physical table.
    """
    # noinspection PyProtectedMember
    from test_tabsdatak.conn.clickhouse._integration_helpers import (
        _ch_client,
        _db_name,
        _drop_table,
    )

    db = _db_name(fresh_clickhouse)
    base = request.node.originalname or request.node.name
    callspec = getattr(request.node, "callspec", None)
    if callspec is not None:
        suffix = (
            callspec.id.replace("-", "_").replace(".", "_")
        )
        tbl = f"itest_{base}_{suffix}"
    else:
        tbl = f"itest_{base}"
    client = _ch_client(fresh_clickhouse)
    _drop_table(client, db, tbl)
    yield db, tbl
    _drop_table(client, db, tbl)
