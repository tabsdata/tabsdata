"""End-to-end Druid integration tests.

The Druid pipeline is two-stage:

  1. Upload parquet to a cloud staging area (S3 / GCS / Azure) using the
     ``td_transporter2`` binary; the staging credentials come from the
     ``DruidDestConn.staging`` CloudLocation.
  2. Submit a native batch ``index_parallel`` ingestion task to the
     Druid Overlord and poll until it terminates.

The Druid cluster is auto-started by the ``druid_config`` session
fixture (see :mod:`conftest`) -- docker compose brings up the full
micro-quickstart stack so no DRUID__* env vars are required. The
S3 staging side still pulls from the shared ``S30__*`` bundle so the
test continues to use the team's existing CI bucket.

The ``td_transporter2`` binary must be installed in the active Python
environment's scripts folder. External prerequisites still gated by
env vars:

  - ``S30__S3_URI``        -- ``s3://bucket[/path/prefix]`` for staging
  - ``S30__S3_REGION``     -- AWS region for staging bucket
  - ``S30__S3_ACCESS_KEY`` / ``S30__S3_SECRET_KEY``

The staging URI's bucket / path are used as the
``DruidDestConn.staging`` base; each test invocation writes under
``<bucket>/<base_path>/td-druid/<trx_id>/<rand>/``. The Druid
datasource each run targets is suffixed with a random id so parallel
test runs never overwrite each other.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    UserPassword,
)
from tabsdatak.conn.druid import DruidDestConn, DruidDest, S3Location

# noinspection PyProtectedMember
import tabsdatak.conn.druid._plugin as plugin
from tabsdatak.spi import DestContext


pytestmark = pytest.mark.druid


# ---------------------------------------------------------------------------
# Env-var-driven gates for the S3 staging side
# ---------------------------------------------------------------------------


try:
    TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)
except BinaryNotFoundError:
    TRANSPORTER_BINARY = None

_S3_URI = os.environ.get("S30__S3_URI")
_S3_REGION = os.environ.get("S30__S3_REGION")
_S3_ACCESS_KEY = os.environ.get("S30__S3_ACCESS_KEY")
_S3_SECRET_KEY = os.environ.get("S30__S3_SECRET_KEY")


_skip_reason = (
    f"Druid integration tests require the {TRANSPORTER_BINARY_NAME} binary "
    "plus an S3 staging bundle (S30__S3_URI, S30__S3_REGION, "
    "S30__S3_ACCESS_KEY, S30__S3_SECRET_KEY)"
)


def _staging_gates_ok() -> bool:
    return all([
        TRANSPORTER_BINARY is not None,
        _S3_URI,
        _S3_REGION,
        _S3_ACCESS_KEY,
        _S3_SECRET_KEY,
    ])


pytestmark = [pytestmark, pytest.mark.skipif(not _staging_gates_ok(), reason=_skip_reason), pytest.mark.xdist_group("druid-plugin_integration_seq")]


# ---------------------------------------------------------------------------
# Test context shims -- concrete subclasses of the SPI abstract classes
# so the integration tests exercise the real ``DestContext`` protocol
# rather than a duck-typed surrogate.
# ---------------------------------------------------------------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path, trx_id: str | None = None) -> None:
        self._work_dir = work_dir
        self._trx_id = trx_id or f"druid-integ-{uuid.uuid4().hex[:8]}"

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "druid-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return self._trx_id

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket[/path]`` into ``(bucket, path)``."""
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


@pytest.fixture
def overlord_url(druid_config) -> str:
    return druid_config.overlord_url


@pytest.fixture
def broker_url(druid_config) -> str:
    return druid_config.broker_url


@pytest.fixture
def staging() -> S3Location:
    bucket, base_path = _parse_s3_uri(_S3_URI)
    return S3Location(
        bucket=bucket,
        region=_S3_REGION,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(_S3_ACCESS_KEY),
            secret_access_key=Secret(_S3_SECRET_KEY),
        ),
        base_path=base_path,
    )


@pytest.fixture
def fresh_druid(overlord_url, staging) -> DruidDestConn:
    """Build a fresh ``DruidDestConn`` whose staging area lives at
    ``s3://$S30__S3_URI`` and whose Overlord points at the auto-started
    docker compose stack."""
    return DruidDestConn(
        overlord_url=overlord_url,
        staging=staging,
        credentials=UserPassword(
            user=Secret("admin"),
            password=Secret(""),
        ),
    )


@pytest.fixture
def datasource_name() -> str:
    return f"td_integ_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def _druid_sql(broker_url: str, sql: str) -> list[dict]:
    """Run a SQL query against the Druid Broker."""
    import requests

    resp = requests.post(
        f"{broker_url}/druid/v2/sql",
        json={"query": sql},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# DruidDestPlugin.write_out -- happy path against a real Druid
# ---------------------------------------------------------------------------


def test_dest_plugin_writes_single_datasource(
    fresh_druid, broker_url, datasource_name, work_dir,
):
    parquet = _make_parquet(
        work_dir / "events.parquet",
        [
            {"id": 1, "name": "alice", "ts": "2026-01-01T00:00:00.000Z"},
            {"id": 2, "name": "bob",   "ts": "2026-01-01T01:00:00.000Z"},
        ],
    )
    dest = DruidDest(
        tables=[datasource_name],
        time_column="ts",
        segment_granularity="HOUR",
    )
    plugin.DruidDestPlugin().write_out(
        _DestCtx(work_dir), fresh_druid, dest, [Path(parquet)],
    )

    # Allow the segments to become queryable (small grace period).
    time.sleep(5)
    rows = _druid_sql(
        broker_url,
        f"SELECT COUNT(*) AS n FROM {datasource_name}",
    )
    assert rows[0]["n"] == 2


def test_dest_plugin_writes_multiple_datasources(
    fresh_druid, broker_url, work_dir,
):
    ds_a = f"td_integ_a_{uuid.uuid4().hex[:8]}"
    ds_b = f"td_integ_b_{uuid.uuid4().hex[:8]}"
    parquet_a = _make_parquet(
        work_dir / "a.parquet",
        [{"id": 1, "ts": "2026-01-01T00:00:00.000Z"}],
    )
    parquet_b = _make_parquet(
        work_dir / "b.parquet",
        [
            {"id": 10, "ts": "2026-01-01T00:00:00.000Z"},
            {"id": 11, "ts": "2026-01-01T01:00:00.000Z"},
        ],
    )
    dest = DruidDest(
        tables=[ds_a, ds_b],
        time_column="ts",
    )
    plugin.DruidDestPlugin().write_out(
        _DestCtx(work_dir), fresh_druid, dest, [
            Path(parquet_a), Path(parquet_b),
        ],
    )

    time.sleep(5)
    rows_a = _druid_sql(broker_url, f"SELECT COUNT(*) AS n FROM {ds_a}")
    rows_b = _druid_sql(broker_url, f"SELECT COUNT(*) AS n FROM {ds_b}")
    assert rows_a[0]["n"] == 1
    assert rows_b[0]["n"] == 2
