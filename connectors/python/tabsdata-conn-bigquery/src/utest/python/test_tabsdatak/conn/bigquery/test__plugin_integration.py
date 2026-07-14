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
  ``base_path``, with ``"staging"`` as the default when absent.

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

from tabsdatak.conn.bigquery import BigQueryDest

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _plugin as plugin
from tabsdatak.spi import DestContext

# noinspection PyProtectedMember
from test_tabsdatak.conn.bigquery._integration_helpers import (
    _bq_client,
    _conn_from,
    _table_fqn,
)

pytestmark = pytest.mark.bigquery


# ---------------------------------------------------------------------------
# Test context shim -- a concrete subclass of the SPI ``DestContext`` so the
# integration tests exercise the real protocol rather than a duck-typed
# surrogate.
# ---------------------------------------------------------------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
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
    conn = _conn_from(cfg)
    dest = BigQueryDest(
        tables=[_table_fqn(cfg, table)],
        if_table_exists="append",
        schema_evolution="update",
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(_dest_ctx(work_dir), conn, dest, [src])
        rows = list(client.query(f"SELECT id, name FROM `{full}` ORDER BY id"))
        assert [(r.id, r.name) for r in rows] == [(1, "alice"), (2, "bob")]
    finally:
        try:
            client.delete_table(full, not_found_ok=True)
        finally:
            client.close()


def test_dest_plugin_bare_table_uses_conn_dataset(fresh_bigquery, work_dir):
    """1-part bare table -- resolved to ``project.dataset.table`` from
    the connection's ``project`` / ``dataset`` defaults."""
    cfg = fresh_bigquery
    table = f"td_bigquery_bare_{uuid.uuid4().hex[:16]}"
    full = f"{cfg.project}.{cfg.dataset}.{table}"

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "name": "alice"}])
    conn = _conn_from(cfg)
    dest = BigQueryDest(tables=[f"`{table}`"])

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(_dest_ctx(work_dir), conn, dest, [src])
        rows = list(client.query(f"SELECT id, name FROM `{full}` ORDER BY id"))
        assert [(r.id, r.name) for r in rows] == [(1, "alice")]
    finally:
        try:
            client.delete_table(full, not_found_ok=True)
        finally:
            client.close()


def test_dest_plugin_two_part_table_uses_conn_project(fresh_bigquery, work_dir):
    """2-part ``dataset.table`` -- the project is filled from the
    connection while the user-supplied dataset is preserved."""
    cfg = fresh_bigquery
    table = f"td_bigquery_twopart_{uuid.uuid4().hex[:16]}"
    full = f"{cfg.project}.{cfg.dataset}.{table}"

    src = _make_parquet(work_dir / "in.parquet", [{"id": 7, "name": "bob"}])
    conn = _conn_from(cfg)
    dest = BigQueryDest(tables=[f"`{cfg.dataset}.{table}`"])

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(_dest_ctx(work_dir), conn, dest, [src])
        rows = list(client.query(f"SELECT id, name FROM `{full}` ORDER BY id"))
        assert [(r.id, r.name) for r in rows] == [(7, "bob")]
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
    conn = _conn_from(cfg)
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir),
        conn,
        BigQueryDest(
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
        tables=[_table_fqn(cfg, table)],
        if_table_exists="replace",
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir),
            conn,
            dest,
            [replacement],
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

    conn = _conn_from(cfg)
    dest = BigQueryDest(
        tables=[_table_fqn(cfg, t1), _table_fqn(cfg, t2)],
    )

    client = _bq_client(cfg)
    try:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir),
            conn,
            dest,
            [src1, src2],
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
