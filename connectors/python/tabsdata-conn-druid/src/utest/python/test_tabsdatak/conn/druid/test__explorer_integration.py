#
#  Copyright 2026. Tabs Data Inc.
#

"""``DruidConnectionExplorer`` catalog tests against a real Druid cluster.

The cluster is auto-started by the ``druid_config`` docker-compose fixture (see
:mod:`conftest`). Druid datasources materialise only on ingestion, so this test
ingests a small dataset itself -- through the connector's own
``DruidDestPlugin`` (parquet staged to S3, then a native batch task) -- and then
exercises the explorer's catalog surface (``check_connect`` / ``metadata`` /
``data``) against the datasource it created. It depends on no pre-seeded tdx
domain (the tdx Druid backend loads no data; a datasource comes into being only
when the connector ingests).

Gated exactly like ``test__plugin_integration.py``: it needs the
``td_transporter`` binary plus the ``S30__*`` S3 staging bundle, and is skipped
otherwise. The explorer itself issues catalog SQL over the Router and never
touches the staging area, so a placeholder staging is enough to build its conn.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pytest
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

import tabsdatak.conn.druid._plugin as plugin
from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.druid import DruidDest, DruidDestConn, S3Location
from tabsdatak.conn.druid.explorer import DruidConnectionExplorer
from tabsdatak.spi import DestContext

pytestmark = pytest.mark.druid


# ---------------------------------------------------------------------------
# Gates: the self-ingest path needs the transporter binary + an S3 staging
# bundle, exactly like test__plugin_integration.py.
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


pytestmark = [
    pytestmark,
    pytest.mark.skipif(not _staging_gates_ok(), reason=_skip_reason),
    pytest.mark.xdist_group("druid-explorer_integration_seq"),
]


# The dataset this test ingests, and the subset of columns the explorer must
# surface (Druid adds a synthetic ``__time`` column on ingestion, so the check
# asserts a subset rather than an exact match).
_ROWS = [
    {"code": "AAA", "name": "Alpha", "continent": "Xanadu", "population": 100,
     "ts": "2026-01-01T00:00:00.000Z"},
    {"code": "BBB", "name": "Bravo", "continent": "Yonder", "population": 200,
     "ts": "2026-01-01T01:00:00.000Z"},
    {"code": "CCC", "name": "Cosmo", "continent": "Zephyr", "population": 300,
     "ts": "2026-01-01T02:00:00.000Z"},
]
_DATA_COLUMNS = {"code", "name", "continent", "population"}

# The explorer only issues catalog SQL over the Router; staging is never used,
# so a placeholder is enough to construct its conn.
_UNUSED_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)


class _DestCtx(DestContext):
    """Concrete ``DestContext`` for driving ``DruidDestPlugin.write_out``."""

    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir
        self._trx_id = f"druid-explorer-{uuid.uuid4().hex[:8]}"

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "druid-explorer-exec-plan"

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


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket[/path]`` into ``(bucket, /path)``."""
    parsed = urlparse(uri)
    return parsed.netloc, "/" + parsed.path.strip("/")


def _dest_conn(overlord_url: str, staging: S3Location) -> DruidDestConn:
    return DruidDestConn(
        overlord_url=overlord_url,
        staging=staging,
        credentials=UserPassword(user=Secret("admin"), password=Secret("")),
    )


def _explorer_conn(router_url: str) -> DruidDestConn:
    # SQL is served by the Router; make_url honors the port carried here.
    return _dest_conn(router_url, _UNUSED_STAGING)


def _druid_sql(broker_url: str, sql: str) -> list[dict]:
    import requests

    resp = requests.post(
        f"{broker_url}/druid/v2/sql", json={"query": sql}, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_fixed(5),
    stop=stop_after_delay(120),
    reraise=True,
)
def _wait_queryable(broker_url: str, datasource: str, expected: int) -> None:
    """Poll ``SELECT COUNT(*)`` until the datasource resolves with all its rows
    (ingestion is asynchronous and segment handoff adds delay)."""
    rows = _druid_sql(broker_url, f"SELECT COUNT(*) AS n FROM {datasource}")
    got = rows[0]["n"] if rows else 0
    assert got == expected, f"{datasource}: expected {expected} rows, got {got}"


def _drop_datasource(overlord_url: str, datasource: str) -> None:
    import requests

    try:
        requests.delete(
            f"{overlord_url}/druid/coordinator/v1/datasources/{datasource}",
            timeout=30,
        )
    except Exception:  # noqa: BLE001 - teardown is best-effort
        pass


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
def ingested(druid_config, staging, tmp_path: Path):
    """Ingest ``_ROWS`` into a unique scratch datasource via the connector, wait
    until it is queryable, and yield ``(datasource, druid_config)``. The
    datasource is dropped on teardown."""
    datasource = f"td_expl_{uuid.uuid4().hex[:12]}"
    work = tmp_path / "work"
    work.mkdir()
    parquet = work / "in.parquet"
    pl.DataFrame(_ROWS).write_parquet(parquet, use_pyarrow=True)
    plugin.DruidDestPlugin().write_out(
        _DestCtx(work),
        _dest_conn(druid_config.overlord_url, staging),
        DruidDest(tables=[datasource], time_column="ts"),
        [parquet],
    )
    _wait_queryable(druid_config.broker_url, datasource, len(_ROWS))
    try:
        yield datasource, druid_config
    finally:
        _drop_datasource(druid_config.overlord_url, datasource)


def test_check_connect_succeeds(druid_config):
    conn = _explorer_conn(druid_config.router_url)
    assert DruidConnectionExplorer().check_connect(conn) is None


def test_metadata_lists_ingested_datasource(ingested):
    datasource, cfg = ingested
    entities = DruidConnectionExplorer().metadata(_explorer_conn(cfg.router_url))
    assert entities
    assert all(isinstance(e, SQLEntity) for e in entities)
    assert all(e.schema == "druid" for e in entities)

    by_table = {e.table: e for e in entities}
    assert datasource in by_table, f"{datasource} not in {sorted(by_table)}"
    found = {c.name for c in by_table[datasource].columns}
    assert _DATA_COLUMNS <= found, f"missing columns: {_DATA_COLUMNS - found}"


def test_data_reads_ingested_rows(ingested):
    datasource, cfg = ingested
    conn = _explorer_conn(cfg.router_url)
    explorer = DruidConnectionExplorer()
    entity = {e.table: e for e in explorer.metadata(conn)}[datasource]

    rows = explorer.data(conn, entity, limit=5)
    assert rows
    assert len(rows) <= 5
    field_names = {c.name for c in entity.columns}
    assert all(set(r) <= field_names for r in rows)
    assert all(_DATA_COLUMNS <= set(r) for r in rows)
