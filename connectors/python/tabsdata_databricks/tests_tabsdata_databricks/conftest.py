#
# Copyright 2025 Tabs Data Inc.
#

from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_databricks.bootest import TESTING_RESOURCES_PATH

from tabsdata.utils.logging import setup_tests_logging

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import logging
import os

import databricks.sdk as dbsdk
import databricks.sql as dbsql
import pytest
from databricks.sdk.core import Config, pat_auth
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
)

logger = logging.getLogger(__name__)

DATABRICKS_CATALOG = os.environ.get("TD_DATABRICKS_CATALOG")
DATABRICKS_HOST = os.environ.get("TD_DATABRICKS_HOST")
DATABRICKS_SCHEMA = os.environ.get("TD_DATABRICKS_SCHEMA")
DATABRICKS_TOKEN = os.environ.get("TD_DATABRICKS_TOKEN")
DATABRICKS_VOLUME = os.environ.get("TD_DATABRICKS_VOLUME")
DATABRICKS_WAREHOUSE_NAME = os.environ.get("TD_DATABRICKS_WAREHOUSE_NAME")


def pytest_configure():
    setup_tests_logging()


def pytest_sessionfinish(session, _exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()


@pytest.fixture(scope="session")
def databricks_client() -> dbsdk.WorkspaceClient:
    ws_client = dbsdk.WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )
    yield ws_client


@pytest.fixture(scope="session")
def sql_conn(databricks_client):
    warehouse_id = None
    for warehouse in databricks_client.warehouses.list():
        if warehouse.name == DATABRICKS_WAREHOUSE_NAME:
            warehouse_id = warehouse.id
            break
    if warehouse_id is None:
        raise ValueError(
            f"Warehouse '{DATABRICKS_WAREHOUSE_NAME}' not found in Databricks"
            " workspace."
        )

    def credentials_provider():
        config = Config(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN,
        )
        return pat_auth(config)

    sql_conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=credentials_provider,
    )
    try:
        yield sql_conn
    finally:
        sql_conn.close()
