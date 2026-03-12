#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
import time
import uuid

import pytest
import requests

import tabsdata as td
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.test_output_druid_list.example import (
    testing_tabsdata_function as output_druid_list_generator,
)
from test_tabsdata.test_output_druid_list_none.example import (
    testing_tabsdata_function as output_druid_list_none_generator,
)
from test_tabsdata.test_output_druid_append.example import (
    testing_tabsdata_function as output_druid_append_generator,
)
from test_tabsdata.test_output_druid_none.example import (
    testing_tabsdata_function as output_druid_none_generator,
)
from test_tabsdata.test_output_druid_time.example import (
    testing_tabsdata_function as output_druid_time_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ── Helpers ───────────────────────────────────────────────────────────────────────────


_STAGING_BASE_PATH = "tabsdata-testing/druid-output"


def _s3_staging(s3_config, uid: str) -> td.S3Config:
    """Build an S3 staging config from the ``s3_config`` fixture."""
    return td.S3Config(
        bucket=s3_config["BUCKET_NAME"],
        region=s3_config["REGION"],
        base_path=f"{_STAGING_BASE_PATH}-{uid}",
        credentials=s3_config["CREDENTIALS"],
    )


def _gcs_staging(gcs_config, uid: str) -> td.GCSConfig:
    """Build a GCS staging config from the ``gcs_config`` fixture."""
    return td.GCSConfig(
        bucket=gcs_config["BUCKET"],
        base_path=f"{_STAGING_BASE_PATH}-{uid}",
        upload_credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
        hmac_credentials=td.HMACCredentials(
            td.EnvironmentSecret(gcs_config["ACCESS_KEY_ENV"]),
            td.EnvironmentSecret(gcs_config["SECRET_KEY_ENV"]),
        ),
    )


def _azure_staging(azure_config, uid: str) -> td.AzureConfig:
    """Build an Azure staging config from the ``azure_config`` fixture."""
    return td.AzureConfig(
        container=azure_config["CONTAINER_NAME"],
        base_path=f"{_STAGING_BASE_PATH}-{uid}",
        credentials=azure_config["CREDENTIALS"],
    )


def _build_conn(druid_config, staging, **kwargs) -> td.DruidConn:
    """Build a DruidConn with the given staging config.

    Extra *kwargs* are forwarded to :class:`td.DruidConn` — use this to
    override tuning parameters such as ``max_concurrent_subtasks``.
    """
    return td.DruidConn(
        overlord_url=druid_config.overlord_url,
        staging=staging,
        credentials=druid_config.credentials,
        **kwargs,
    )


def _query_datasource(
    druid_config, datasource: str, retries: int = 10, delay: float = 2.0
) -> list[dict]:
    """Query a Druid datasource via the Router SQL endpoint.

    Retries on failure because Druid segments may not be immediately
    queryable after an ingestion task reports SUCCESS — the Historical
    node needs time to load them from deep storage.
    """
    last_resp = None
    for attempt in range(retries):
        resp = requests.post(
            f"{druid_config.router_url}/druid/v2/sql",
            json={"query": f'SELECT * FROM "{datasource}"'},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.ok:
            return resp.json()
        last_resp = resp
        logger.warning(
            "Query for '%s' returned %d (attempt %d/%d), retrying in %.1fs",
            datasource,
            resp.status_code,
            attempt + 1,
            retries,
            delay,
        )
        if attempt < retries - 1:
            time.sleep(delay)
    last_resp.raise_for_status()


def _poll_for_row_count(
    druid_config, datasource: str, expected_count: int,
    retries: int = 20, delay: float = 3.0,
) -> list[dict]:
    """Poll a Druid datasource until the expected row count is reached.

    After an append operation, newly ingested segments may not be
    immediately queryable even though the ingestion task has reported
    SUCCESS — the Historical node needs time to discover, download and
    load them from deep storage.  This helper retries the query until
    the expected number of rows appears or the retry budget is exhausted.
    """
    rows: list[dict] = []
    for attempt in range(retries):
        rows = _query_datasource(druid_config, datasource)
        if len(rows) >= expected_count:
            return rows
        logger.info(
            "Waiting for %d rows in '%s', currently %d (attempt %d/%d)",
            expected_count, datasource, len(rows), attempt + 1, retries,
        )
        if attempt < retries - 1:
            time.sleep(delay)
    return rows  # return whatever we got; caller will assert and fail


def _normalize_druid_rows(rows: list[dict]):
    """Convert Druid SQL result rows into a normalized Polars DataFrame.

    Drops the Druid-specific ``__time`` column and applies the same
    cleaning as :func:`read_json_and_clean` (remove ``$td.*`` columns,
    sort columns alphabetically, sort rows, lowercase column names).
    """
    import polars as pl

    df = pl.DataFrame(rows)
    if "__time" in df.columns:
        df = df.drop("__time")
    return clean_polars_df(df)


def _load_expected_outputs():
    """Load the two expected result DataFrames for the druid_list test.

    Returns (expected_table1, expected_table2) — normalized Polars
    DataFrames that mirror the two different transformations applied by
    the ``output_druid_list`` example function.
    """
    base = os.path.join(TESTING_RESOURCES_FOLDER, "test_output_druid_list")
    expected_1 = read_json_and_clean(os.path.join(base, "expected_result.json"))
    expected_2 = read_json_and_clean(os.path.join(base, "expected_result_2.json"))
    return expected_1, expected_2


def _delete_datasource(druid_config, datasource: str) -> None:
    """Best-effort deletion of a Druid datasource via the Coordinator API.

    Disables the datasource and marks all its segments for deletion.
    Failures are logged but never raised — cleanup is non-fatal.
    """
    try:
        resp = requests.delete(
            f"{druid_config.overlord_url}/druid/coordinator/v1/datasources/{datasource}",
            timeout=10,
        )
        if resp.ok:
            logger.info("Cleaned up datasource '%s'", datasource)
        else:
            logger.debug(
                "Cleanup of '%s' returned %d (may not exist yet)",
                datasource,
                resp.status_code,
            )
    except Exception:
        logger.debug("Cleanup of '%s' failed (non-fatal)", datasource, exc_info=True)


# ── Tests ─────────────────────────────────────────────────────────────────────────────
#
# Worker slot budget: each ingestion task occupies 1 MiddleManager worker
# slot.  When max_concurrent_subtasks > 1 the supervisor spawns real
# subtasks: a test with T tables and N subtasks needs T*(N+1) slots when
# N>1, or T*1 when N=1 (inline).  The bulk loader submits ALL table tasks
# concurrently, then polls them together.
#
# The Docker test cluster is configured with druid_worker_capacity=16
# (matching the default pytest-xdist worker count).  See each test's
# docstring for its slot usage.


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "cloud_fixture, staging_factory, label",
    [
        pytest.param("s3_config", _s3_staging, "s3", marks=pytest.mark.s3),
        pytest.param("gcs_config", _gcs_staging, "gcs", marks=pytest.mark.gcs),
        pytest.param("azure_config", _azure_staging, "azure", marks=pytest.mark.azure),
    ],
)
def test_output_druid_staging(
    druid_config, cloud_fixture, staging_factory, label, request,
):
    """Single-table write staged through each cloud backend. (1 slot)"""
    cloud_config = request.getfixturevalue(cloud_fixture)
    uid = uuid.uuid4().hex[:12]
    datasource = f"druid_output_{label}_{uid}"
    request.addfinalizer(lambda: _delete_datasource(druid_config, datasource))

    conn = _build_conn(druid_config, staging_factory(cloud_config, uid))
    fn = output_druid_append_generator(conn=conn, destination_table=datasource)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    expected = read_json_and_clean(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_druid_list",
            "expected_result.json",
        )
    )
    rows = _query_datasource(druid_config, datasource)
    actual = _normalize_druid_rows(rows)
    assert actual.equals(expected), (
        f"Content mismatch ({label} staging) in '{datasource}':\n"
        f"Expected:\n{expected}\n"
        f"Actual:\n{actual}"
    )


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_list(druid_config, s3_config, request):
    """Two-table write with list destination. (2 slots)"""
    uid = uuid.uuid4().hex[:12]
    table1 = f"druid_output_list_{uid}"
    table2 = f"second_druid_output_list_{uid}"
    for t in [table1, table2]:
        request.addfinalizer(lambda ds=t: _delete_datasource(druid_config, ds))

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid))
    testing_function = output_druid_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    expected_1, expected_2 = _load_expected_outputs()

    for datasource, expected in [(table1, expected_1), (table2, expected_2)]:
        rows = _query_datasource(druid_config, datasource)
        actual = _normalize_druid_rows(rows)
        assert actual.equals(expected), (
            f"Content mismatch in '{datasource}':\n"
            f"Expected:\n{expected}\n"
            f"Actual:\n{actual}"
        )


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_replace(druid_config, s3_config, request):
    """Writing with if_table_exists='replace' drops existing segments. (2 slots)"""
    uid = uuid.uuid4().hex[:12]
    table1 = f"druid_output_replace_{uid}"
    table2 = f"second_druid_output_replace_{uid}"
    for t in [table1, table2]:
        request.addfinalizer(lambda ds=t: _delete_datasource(druid_config, ds))

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid))
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list",
        "mock_table.parquet",
    )
    expected_1, expected_2 = _load_expected_outputs()

    # First write (append mode)
    testing_function_append = output_druid_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    fnrun = tdfnrun(function=testing_function_append, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    for datasource, expected in [(table1, expected_1), (table2, expected_2)]:
        actual = _normalize_druid_rows(
            _query_datasource(druid_config, datasource)
        )
        assert actual.equals(expected), (
            f"Content mismatch after first write in '{datasource}':\n"
            f"Expected:\n{expected}\n"
            f"Actual:\n{actual}"
        )

    # Second write in replace mode — content should be identical
    testing_function_replace = output_druid_list_generator(
        conn=conn,
        destination_table=[table1, table2],
        if_table_exists="replace",
    )
    fnrun2 = tdfnrun(function=testing_function_replace, input_tables=mock_parquet_table)
    assert fnrun2.status == 0

    for datasource, expected in [(table1, expected_1), (table2, expected_2)]:
        actual = _normalize_druid_rows(
            _query_datasource(druid_config, datasource)
        )
        assert actual.equals(expected), (
            f"Content mismatch after replace in '{datasource}':\n"
            f"Expected:\n{expected}\n"
            f"Actual:\n{actual}"
        )


def _datasource_exists(druid_config, datasource: str) -> bool:
    """Check if a Druid datasource exists via the Router SQL endpoint."""
    try:
        resp = requests.post(
            f"{druid_config.router_url}/druid/v2/sql",
            json={"query": f'SELECT COUNT(*) AS cnt FROM "{datasource}"'},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        # Druid returns 4xx when the datasource doesn't exist
        return resp.ok
    except Exception:
        return False


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_list_none(druid_config, s3_config):
    """When all results are None, no datasources should be created. (0 slots)"""
    uid = uuid.uuid4().hex[:12]
    table1 = f"druid_output_list_none_{uid}"
    table2 = f"second_druid_output_list_none_{uid}"

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid))
    testing_function = output_druid_list_none_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    # Neither datasource should have been created (all results were None)
    for datasource in [table1, table2]:
        assert not _datasource_exists(druid_config, datasource), (
            f"Datasource '{datasource}' should not exist after a None result"
        )


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_none(druid_config, s3_config):
    """When a single result is None, no datasource should be created. (0 slots)"""
    uid = uuid.uuid4().hex[:12]
    datasource = f"druid_output_none_{uid}"

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid))
    testing_function = output_druid_none_generator(
        conn=conn,
        destination_table=datasource,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_none",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    assert not _datasource_exists(druid_config, datasource), (
        f"Datasource '{datasource}' should not exist after a None result"
    )


# ── Parallel subtask tests ───────────────────────────────────────────────────────


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_parallel_subtasks(druid_config, s3_config, request):
    """Exercises index_parallel with maxNumConcurrentSubTasks=2. (4 slots)"""
    uid = uuid.uuid4().hex[:12]
    table1 = f"druid_output_parallel_{uid}"
    table2 = f"second_druid_output_parallel_{uid}"
    for t in [table1, table2]:
        request.addfinalizer(lambda ds=t: _delete_datasource(druid_config, ds))

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid), max_concurrent_subtasks=2)
    testing_function = output_druid_list_generator(
        conn=conn,
        destination_table=[table1, table2],
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    expected_1, expected_2 = _load_expected_outputs()

    for datasource, expected in [(table1, expected_1), (table2, expected_2)]:
        rows = _query_datasource(druid_config, datasource)
        actual = _normalize_druid_rows(rows)
        assert actual.equals(expected), (
            f"Content mismatch in '{datasource}':\n"
            f"Expected:\n{expected}\n"
            f"Actual:\n{actual}"
        )


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_append(druid_config, s3_config, request):
    """Writing twice with if_table_exists='append' doubles the rows. (1 slot)"""
    uid = uuid.uuid4().hex[:12]
    datasource = f"druid_output_append_{uid}"
    request.addfinalizer(lambda: _delete_datasource(druid_config, datasource))

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid))
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_list",
        "mock_table.parquet",
    )
    # expected_result.json from druid_list is the drop_nulls() result (5 rows)
    expected = read_json_and_clean(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_druid_list",
            "expected_result.json",
        )
    )
    expected_row_count = len(expected)

    # First write (append mode — the default)
    fn1 = output_druid_append_generator(conn=conn, destination_table=datasource)
    fnrun1 = tdfnrun(function=fn1, input_tables=mock_parquet_table)
    assert fnrun1.status == 0

    rows_after_first = _query_datasource(druid_config, datasource)
    actual_first = _normalize_druid_rows(rows_after_first)
    assert actual_first.equals(expected), (
        f"Content mismatch after first append:\n"
        f"Expected:\n{expected}\n"
        f"Actual:\n{actual_first}"
    )

    # Second write (append mode again — rows should double)
    fn2 = output_druid_append_generator(conn=conn, destination_table=datasource)
    fnrun2 = tdfnrun(function=fn2, input_tables=mock_parquet_table)
    assert fnrun2.status == 0

    # After the second append, newly ingested segments may not be
    # immediately queryable (Historical needs to load them from deep
    # storage).  Poll until the expected total appears.
    rows_after_second = _poll_for_row_count(
        druid_config, datasource, 2 * expected_row_count,
    )
    assert len(rows_after_second) == 2 * expected_row_count, (
        f"Expected {2 * expected_row_count} rows after two appends, "
        f"got {len(rows_after_second)}"
    )


@pytest.mark.druid
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.s3
def test_output_druid_time(druid_config, s3_config, request):
    """Ingestion with an explicit time_column maps real timestamps to __time. (1 slot)"""
    uid = uuid.uuid4().hex[:12]
    datasource = f"druid_output_time_{uid}"
    request.addfinalizer(lambda: _delete_datasource(druid_config, datasource))

    conn = _build_conn(druid_config, _s3_staging(s3_config, uid), time_column="event_time")
    testing_function = output_druid_time_generator(
        conn=conn,
        destination_table=datasource,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_druid_time",
        "mock_table.parquet",
    )
    fnrun = tdfnrun(function=testing_function, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    rows = _query_datasource(druid_config, datasource)

    # Verify __time contains real timestamps (not the 2000-01-01 dummy)
    dummy_time = "2000-01-01T00:00:00.000Z"
    for row in rows:
        assert "__time" in row, f"Row missing '__time': {row}"
        assert row["__time"] != dummy_time, (
            f"__time should be a real timestamp when time_column is set, "
            f"got dummy value {dummy_time}"
        )

    # Verify non-time data columns match expected output
    expected = read_json_and_clean(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_druid_time",
            "expected_result.json",
        )
    )
    actual = _normalize_druid_rows(rows)
    assert actual.equals(expected), (
        f"Content mismatch in '{datasource}':\n"
        f"Expected:\n{expected}\n"
        f"Actual:\n{actual}"
    )
