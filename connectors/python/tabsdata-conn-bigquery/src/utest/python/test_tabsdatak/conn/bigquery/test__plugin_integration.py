"""End-to-end BigQuery plugin tests against real BigQuery + GCS.

BigQuery and GCS are managed services -- there is no per-test
docker-based provisioning fixture like the ones the SQL connectors
get. These tests therefore opt-in via a small set of environment
variables that, when set, point the plugin at a real workspace:

- ``GCP0__GCP_SERVICE_ACCOUNT_KEY`` -- service-account-key JSON (the
  contents of the file, not a path); used for both BigQuery API auth
  and GCS staging uploads.
- ``GCP0__BIGQUERY_PROJECT`` -- the GCP project id.
- ``GCP0__BIGQUERY_DATASET`` -- the target BigQuery dataset (must
  already exist in the project; the plugin creates tables under it
  but not datasets).
- ``GCP0__GCP_STORAGE_URI`` -- the GCS staging location as a
  ``gs://<bucket>[/<base_path>]`` URI. The bucket portion is fed to
  :class:`GCSLocation.bucket`; the path portion (if any) becomes
  ``base_path``, with ``"staging/"`` as the default when absent.

The ``GCP0__GCP_STORAGE_ACCESS_KEY`` / ``GCP0__GCP_STORAGE_SECRET_KEY``
HMAC pair is intentionally NOT consumed here -- BigQuery's staging
goes through the native GCS API on the service-account JSON.

When any of the required variables is missing all tests in this
file are skipped at collection time. Error-path coverage
(fault-injection via mocks) lives in ``test__plugin.py``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryConn, BigQueryDest, GCSLocation

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _plugin as plugin
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.spi import DestContext


pytestmark = pytest.mark.bigquery


# ---------------------------------------------------------------------------
# Test context shim -- a concrete subclass of the SPI ``DestContext`` so the
# integration tests exercise the real protocol rather than a duck-typed
# surrogate.
# ---------------------------------------------------------------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "bigquery-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "bigquery-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


def _dest_ctx(work: Path) -> _DestCtx:
    return _DestCtx(work)


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Real-BigQuery config fixture
# ---------------------------------------------------------------------------


_PREFIX = "GCP0"

_REQUIRED_ENV = [
    f"{_PREFIX}__GCP_SERVICE_ACCOUNT_KEY",
    f"{_PREFIX}__BIGQUERY_PROJECT",
    f"{_PREFIX}__BIGQUERY_DATASET",
    f"{_PREFIX}__GCP_STORAGE_URI",
]


class _BqConfig:
    def __init__(
        self,
        *,
        sa_json: str,
        project: str,
        dataset: str,
        bucket: str,
        base_path: str,
    ) -> None:
        self.sa_json = sa_json
        self.project = project
        self.dataset = dataset
        self.bucket = bucket
        self.base_path = base_path


def _split_gs_uri(uri: str) -> tuple[str, str]:
    """Split ``gs://<bucket>[/<path>]`` into ``(bucket, base_path)``.

    Path defaults to ``"staging/"`` when the URI carries only a bucket.
    The returned ``base_path`` is normalized to always end with ``/``
    so callers don't have to worry about ``GCSLocation.full_path``
    join semantics.
    """
    prefix = "gs://"
    if not uri.startswith(prefix):
        raise ValueError(
            f"{_PREFIX}__GCP_STORAGE_URI must start with gs://: {uri!r}"
        )
    rest = uri[len(prefix):].strip("/")
    if "/" in rest:
        bucket, base_path = rest.split("/", 1)
        base_path = base_path.strip("/") + "/"
    else:
        bucket, base_path = rest, "staging/"
    return bucket, base_path


@pytest.fixture
def fresh_bigquery() -> _BqConfig:
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "BigQuery integration tests require environment variables: "
            + ", ".join(missing)
        )
    bucket, base_path = _split_gs_uri(os.environ[f"{_PREFIX}__GCP_STORAGE_URI"])
    return _BqConfig(
        sa_json=os.environ[f"{_PREFIX}__GCP_SERVICE_ACCOUNT_KEY"],
        project=os.environ[f"{_PREFIX}__BIGQUERY_PROJECT"],
        dataset=os.environ[f"{_PREFIX}__BIGQUERY_DATASET"],
        bucket=bucket,
        base_path=base_path,
    )


def _conn_from(cfg: _BqConfig) -> BigQueryConn:
    return BigQueryConn(
        staging=GCSLocation(
            bucket=cfg.bucket,
            credentials=GCPJson(json=Secret.lit(cfg.sa_json)),
            base_path=cfg.base_path,
        ),
        credentials=GCPJson(json=Secret.lit(cfg.sa_json)),
        project=cfg.project,
        dataset=cfg.dataset,
    )


def _table_fqn(cfg: _BqConfig, table: str) -> str:
    """Build the user-facing FQN (outer-only backticks per BigQuery
    syntax). The plugin strips the wrapping backticks before
    forwarding to the SDK."""
    return f"`{cfg.project}.{cfg.dataset}.{table}`"


def _bq_client(cfg: _BqConfig):
    from google.cloud import bigquery
    import json as _json

    return bigquery.Client.from_service_account_info(
        _json.loads(cfg.sa_json),
        project=cfg.project,
    )


# ---------------------------------------------------------------------------
# BigQueryDestPlugin.write_out -- happy path against real BigQuery
# ---------------------------------------------------------------------------


def test_dest_plugin_write_out_creates_and_appends(fresh_bigquery, work_dir):
    cfg = fresh_bigquery
    table = f"td_bigquery_test_{uuid.uuid4().hex[:16]}"
    full = f"{cfg.project}.{cfg.dataset}.{table}"

    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    dest = BigQueryDest(
        conn=_conn_from(cfg),
        tables=[_table_fqn(cfg, table)],
        if_table_exists="append",
        schema_evolution="update",
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
        rows = list(client.query(f"SELECT id, name FROM `{full}` ORDER BY id"))
        assert [(r.id, r.name) for r in rows] == [(1, "alice"), (2, "bob")]
    finally:
        try:
            client.delete_table(full, not_found_ok=True)
        finally:
            client.close()


def test_dest_plugin_write_out_replace_clears_existing(fresh_bigquery, work_dir):
    cfg = fresh_bigquery
    table = f"td_bigquery_replace_{uuid.uuid4().hex[:16]}"
    full = f"{cfg.project}.{cfg.dataset}.{table}"

    # Seed with one row in append mode, then overwrite with replace.
    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 99, "name": "stale"}],
    )
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir),
        BigQueryDest(
            conn=_conn_from(cfg),
            tables=[_table_fqn(cfg, table)],
            if_table_exists="append",
        ),
        [seed],
    )

    replacement = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "fresh"}],
    )
    dest = BigQueryDest(
        conn=_conn_from(cfg),
        tables=[_table_fqn(cfg, table)],
        if_table_exists="replace",
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [replacement],
        )
        rows = list(client.query(f"SELECT id, name FROM `{full}` ORDER BY id"))
        assert [(r.id, r.name) for r in rows] == [(1, "fresh")]
    finally:
        try:
            client.delete_table(full, not_found_ok=True)
        finally:
            client.close()


def test_dest_plugin_write_out_multiple_tables(fresh_bigquery, work_dir):
    cfg = fresh_bigquery
    t1 = f"td_bigquery_multi_a_{uuid.uuid4().hex[:16]}"
    t2 = f"td_bigquery_multi_b_{uuid.uuid4().hex[:16]}"
    full1 = f"{cfg.project}.{cfg.dataset}.{t1}"
    full2 = f"{cfg.project}.{cfg.dataset}.{t2}"

    src1 = _make_parquet(work_dir / "in1.parquet", [{"x": 10}])
    src2 = _make_parquet(work_dir / "in2.parquet", [{"y": "v"}])

    dest = BigQueryDest(
        conn=_conn_from(cfg),
        tables=[_table_fqn(cfg, t1), _table_fqn(cfg, t2)],
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src1, src2],
        )
        rows1 = list(client.query(f"SELECT x FROM `{full1}`"))
        rows2 = list(client.query(f"SELECT y FROM `{full2}`"))
        assert [r.x for r in rows1] == [10]
        assert [r.y for r in rows2] == ["v"]
    finally:
        for full in (full1, full2):
            try:
                client.delete_table(full, not_found_ok=True)
            except Exception:
                pass
        client.close()
