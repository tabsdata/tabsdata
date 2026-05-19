
from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    UserPassword,
)
from tabsdatak.conn.druid import DruidConn, DruidDest, S3Location

# noinspection PyProtectedMember
import tabsdatak.conn.druid._plugin as plugin
from tabsdatak.spi import DestContext, ParquetFile


pytestmark = pytest.mark.druid


# ---------------------------------------------------------------------------
# Env-driven gates for the S3 staging side
# ---------------------------------------------------------------------------


_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")

_S3_URI = os.environ.get("S30__S3_URI")
_S3_REGION = os.environ.get("S30__S3_REGION")
_S3_ACCESS_KEY = os.environ.get("S30__S3_ACCESS_KEY")
_S3_SECRET_KEY = os.environ.get("S30__S3_SECRET_KEY")


_skip_reason = (
    "Druid integration tests require TD_TRANSPORTER_BIN plus an S3 staging "
    "bundle (S30__S3_URI, S30__S3_REGION, S30__S3_ACCESS_KEY, "
    "S30__S3_SECRET_KEY)"
)


def _staging_gates_ok() -> bool:
    return all([
        _TRANSPORTER_BIN and Path(_TRANSPORTER_BIN).is_file(),
        _S3_URI,
        _S3_REGION,
        _S3_ACCESS_KEY,
        _S3_SECRET_KEY,
    ])


pytestmark = [pytestmark, pytest.mark.skipif(not _staging_gates_ok(), reason=_skip_reason)]


# ---------------------------------------------------------------------------
# Test context shims -- concrete subclasses of the SPI abstract classes
# so the integration tests exercise the real ``DestContext`` protocol
# rather than a duck-typed surrogate.
# ---------------------------------------------------------------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path, trx_id: str | None = None) -> None:
        self._work_dir = work_dir
        self._trx_id = trx_id or f"druid-integ-{uuid.uuid4().hex[:8]}"

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _parse_s3_uri(uri: str) -> tuple[str, str]:
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
            access_key_id=Secret.lit(_S3_ACCESS_KEY),
            secret_access_key=Secret.lit(_S3_SECRET_KEY),
        ),
        base_path=base_path,
    )


@pytest.fixture
def fresh_druid(overlord_url, staging) -> DruidConn:
    return DruidConn(
        overlord_url=overlord_url,
        staging=staging,
        credentials=UserPassword(
            user=Secret.lit("admin"),
            password=Secret.lit(""),
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
        conn=fresh_druid,
        tables=[datasource_name],
        time_column="ts",
        segment_granularity="HOUR",
    )
    plugin.DruidDestPlugin().write_out(
        _DestCtx(work_dir), dest, [ParquetFile(parquet)],
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
        conn=fresh_druid,
        tables=[ds_a, ds_b],
        time_column="ts",
    )
    plugin.DruidDestPlugin().write_out(
        _DestCtx(work_dir), dest, [
            ParquetFile(parquet_a), ParquetFile(parquet_b),
        ],
    )

    time.sleep(5)
    rows_a = _druid_sql(broker_url, f"SELECT COUNT(*) AS n FROM {ds_a}")
    rows_b = _druid_sql(broker_url, f"SELECT COUNT(*) AS n FROM {ds_b}")
    assert rows_a[0]["n"] == 1
    assert rows_b[0]["n"] == 2
