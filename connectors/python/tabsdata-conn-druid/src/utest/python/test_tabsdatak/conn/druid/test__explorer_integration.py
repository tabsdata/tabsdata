#
#  Copyright 2026 Tabsdata Inc.
#

"""``DruidConnectionExplorer`` catalog tests against a tdx-provisioned Druid.

Uses the domain/system fixture model: the ``druid`` system and the
``druid_world`` domain are provisioned by tdx (see
:mod:`tabsdatax.itest.fixture.backends.druid`) and injected as typed
dataclasses. A Druid domain names a datasource and carries the cluster's
overlord / router HTTP endpoints plus the admin basic-auth credentials.

A datasource only materialises on ingestion, and ``druid_world`` is created
empty (tdx never stages or ingests). These tests therefore seed the domain's
datasource themselves before exercising the explorer, and drop it afterwards.
Seeding goes straight to the overlord as a native batch ``index_parallel``
task with an ``inline`` input source -- no cloud staging is involved -- so the
explorer's catalog surface (``check_connect`` / ``metadata`` / ``data``) is
driven over the router SQL endpoint using only the endpoints the domain
already carries.
"""

from __future__ import annotations

import json

import pytest
from tabsdatax.itest.fixture.backends.druid import (
    DruidDomain,
    DruidSystem,
    druid_domain,
    druid_system,
)
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.druid import DruidDestConn, S3Location
from tabsdatak.conn.druid._explorer import DruidConnectionExplorer

pytestmark = [
    pytest.mark.druid,
    # All three tests seed and drop the same shared domain datasource, so they
    # must not run concurrently against it.
    pytest.mark.xdist_group("druid-explorer_integration_seq"),
]


# The dataset seeded into the domain's datasource, and the subset of columns
# the explorer must surface (Druid adds a synthetic ``__time`` column on
# ingestion, so the checks assert a subset rather than an exact match).
_TIME_COLUMN = "ts"
_ROWS = [
    {
        "code": "AAA",
        "name": "Alpha",
        "continent": "Xanadu",
        "population": 100,
        "ts": "2026-01-01T00:00:00.000Z",
    },
    {
        "code": "BBB",
        "name": "Bravo",
        "continent": "Yonder",
        "population": 200,
        "ts": "2026-01-01T01:00:00.000Z",
    },
    {
        "code": "CCC",
        "name": "Cosmo",
        "continent": "Zephyr",
        "population": 300,
        "ts": "2026-01-01T02:00:00.000Z",
    },
]
_DATA_COLUMNS = {"code", "name", "continent", "population"}

# The explorer only issues catalog SQL over the router; staging is never used,
# so a placeholder is enough to construct its conn.
_UNUSED_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)


def _auth(domain: DruidDomain) -> tuple[str, str]:
    return (domain.user, domain.password)


def _explorer_conn(domain: DruidDomain) -> DruidDestConn:
    # SQL is served by the router; make_url honors the port carried here, so
    # point the conn at the router URL.
    return DruidDestConn(
        overlord_url=domain.router_url,
        staging=_UNUSED_STAGING,
        credentials=UserPassword(
            user=Secret(domain.user), password=Secret(domain.password)
        ),
    )


def _druid_sql(router_url: str, sql: str, auth: tuple[str, str]) -> list[dict]:
    import requests

    resp = requests.post(
        f"{router_url}/druid/v2/sql", json={"query": sql}, auth=auth, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def _submit_inline_ingest(
    overlord_url: str, datasource: str, rows: list[dict], auth: tuple[str, str]
) -> str:
    """Submit a native batch ``index_parallel`` task that ingests ``rows``
    inline (schema auto-discovered) and return its task id."""
    import requests

    task = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {"column": _TIME_COLUMN, "format": "iso"},
                "dimensionsSpec": {"useSchemaDiscovery": True},
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "day",
                    "queryGranularity": "none",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "inline",
                    "data": "\n".join(json.dumps(r) for r in rows),
                },
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {"type": "index_parallel"},
        },
    }
    resp = requests.post(
        f"{overlord_url}/druid/indexer/v1/task", json=task, auth=auth, timeout=30
    )
    resp.raise_for_status()
    return resp.json()["task"]


@retry(
    retry=retry_if_exception_type(AssertionError),
    wait=wait_fixed(3),
    stop=stop_after_delay(120),
    reraise=True,
)
def _wait_task_success(overlord_url: str, task_id: str, auth: tuple[str, str]) -> None:
    """Poll the ingestion task until it succeeds; fail fast if it errors."""
    import requests

    resp = requests.get(
        f"{overlord_url}/druid/indexer/v1/task/{task_id}/status", auth=auth, timeout=30
    )
    resp.raise_for_status()
    status = resp.json()["status"]["status"]
    if status in ("FAILED", "UNKNOWN"):
        raise RuntimeError(f"ingestion task {task_id} ended {status}")
    assert status == "SUCCESS", f"ingestion task {task_id} still {status}"


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_fixed(5),
    stop=stop_after_delay(120),
    reraise=True,
)
def _wait_queryable(
    router_url: str, datasource: str, expected: int, auth: tuple[str, str]
) -> None:
    """Poll ``SELECT COUNT(*)`` until the datasource resolves with all its rows
    (segment handoff lags task success)."""
    rows = _druid_sql(router_url, f'SELECT COUNT(*) AS n FROM "{datasource}"', auth)
    got = rows[0]["n"] if rows else 0
    assert got == expected, f"{datasource}: expected {expected} rows, got {got}"


def _drop_datasource(overlord_url: str, datasource: str, auth: tuple[str, str]) -> None:
    import requests

    try:
        requests.delete(
            f"{overlord_url}/druid/coordinator/v1/datasources/{datasource}",
            auth=auth,
            timeout=30,
        )
    except Exception:  # noqa: BLE001 - teardown is best-effort
        pass


@pytest.fixture
def seeded_world(domain_druid_world: DruidDomain):
    """Ingest ``_ROWS`` into the domain's datasource via an inline native batch
    task, wait until it is queryable, and yield the datasource name. The
    datasource is dropped before (in case a crashed prior run left segments
    that would skew the counts) and after."""
    domain = domain_druid_world
    auth = _auth(domain)
    datasource = domain.datasource
    _drop_datasource(domain.overlord_url, datasource, auth)
    task_id = _submit_inline_ingest(domain.overlord_url, datasource, _ROWS, auth)
    _wait_task_success(domain.overlord_url, task_id, auth)
    _wait_queryable(domain.router_url, datasource, len(_ROWS), auth)
    try:
        yield datasource
    finally:
        _drop_datasource(domain.overlord_url, datasource, auth)


@druid_system("druid")
@druid_domain("druid_world")
def test_check_connect_succeeds(
    system_druid: DruidSystem, domain_druid_world: DruidDomain
):
    conn = _explorer_conn(domain_druid_world)
    assert DruidConnectionExplorer().check_connect(conn) is None


@druid_system("druid")
@druid_domain("druid_world")
def test_metadata_lists_seeded_datasource(
    system_druid: DruidSystem, domain_druid_world: DruidDomain, seeded_world: str
):
    entities = DruidConnectionExplorer().metadata(_explorer_conn(domain_druid_world))
    assert entities
    assert all(isinstance(e, SQLEntity) for e in entities)
    assert all(e.schema == "druid" for e in entities)

    by_table = {e.table: e for e in entities}
    assert seeded_world in by_table, f"{seeded_world} not in {sorted(by_table)}"
    found = {c.name for c in by_table[seeded_world].columns}
    assert _DATA_COLUMNS <= found, f"missing columns: {_DATA_COLUMNS - found}"


@druid_system("druid")
@druid_domain("druid_world")
def test_data_reads_ingested_rows(
    system_druid: DruidSystem, domain_druid_world: DruidDomain, seeded_world: str
):
    conn = _explorer_conn(domain_druid_world)
    explorer = DruidConnectionExplorer()
    entity = {e.table: e for e in explorer.metadata(conn)}[seeded_world]

    rows = explorer.data(conn, entity, limit=5)
    assert rows
    assert len(rows) <= 5
    field_names = {c.name for c in entity.columns}
    assert all(set(r) <= field_names for r in rows)
    assert all(_DATA_COLUMNS <= set(r) for r in rows)
