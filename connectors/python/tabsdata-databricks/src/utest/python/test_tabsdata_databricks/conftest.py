#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdata_databricks.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

from typing import Any, Generator

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)

pytest_plugins = ["test_tabsdata.fntest"]


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
        # No need to download, the master process has already done that.
        return
    clean_everything()


@pytest.fixture(scope="session")
def databricks_client(databricks_config: dict) -> Generator[Any, Any, None]:
    import databricks.sdk as dbsdk  # Lazy import

    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    ws_client = dbsdk.WorkspaceClient(
        host=host,
        token=token,
    )
    yield ws_client


@pytest.fixture(scope="session")
def databricks_config() -> Generator[dict[str, str], Any, None]:
    import os  # Lazy import

    config = {
        "CATALOG_ENV": "DBX0__CATALOG",
        "HOST_ENV": "DBX0__HOST",
        "SCHEMA_ENV": "DBX0__SCHEMA",
        "TOKEN_ENV": "DBX0__TOKEN",
        "VOLUME_ENV": "DBX0__VOLUME",
        "WAREHOUSE_NAME_ENV": "DBX0__WAREHOUSE_NAME",
    }
    host = os.environ.get(config["HOST_ENV"])
    token = os.environ.get(config["TOKEN_ENV"])
    if not host or not token:
        raise Exception(
            f"The environment variables {config['HOST_ENV']} and/or "
            f"{config['TOKEN_ENV']} are not set. Unable to run tests "
            "using the 'databricks_config' fixture."
        )
    config["HOST"] = host
    config["TOKEN"] = token
    if not os.environ.get(config["CATALOG_ENV"]):
        raise Exception(
            f"The environment variable {config['CATALOG_ENV']} is not set. "
            "Unable to run tests using the 'databricks_config' fixture."
        )
    config["CATALOG"] = os.environ.get(config["CATALOG_ENV"])
    if not os.environ.get(config["SCHEMA_ENV"]):
        raise Exception(
            f"The environment variable {config['SCHEMA_ENV']} is not set. "
            "Unable to run tests using the 'databricks_config' fixture."
        )
    config["SCHEMA"] = os.environ.get(config["SCHEMA_ENV"])
    if not os.environ.get(config["VOLUME_ENV"]):
        raise Exception(
            f"The environment variable {config['VOLUME_ENV']} is not set. "
            "Unable to run tests using the 'databricks_config' fixture."
        )
    config["VOLUME"] = os.environ.get(config["VOLUME_ENV"])
    if not os.environ.get(config["WAREHOUSE_NAME_ENV"]):
        raise Exception(
            f"The environment variable {config['WAREHOUSE_NAME_ENV']} is not set. "
            "Unable to run tests using the 'databricks_config' fixture."
        )
    config["WAREHOUSE_NAME"] = os.environ.get(config["WAREHOUSE_NAME_ENV"])
    yield config


@pytest.fixture(scope="session")
def sql_conn(databricks_client, databricks_config):
    import databricks.sql as dbsql  # Lazy import
    from databricks.sdk.core import Config, pat_auth  # Lazy import

    warehouse_id = None
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    for warehouse in databricks_client.warehouses.list():
        if warehouse.name == warehouse_name:
            warehouse_id = warehouse.id
            break
    if warehouse_id is None:
        raise ValueError(
            f"Warehouse '{warehouse_name}' not found in Databricks workspace."
        )

    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]

    def credentials_provider():
        config = Config(
            host=host,
            token=token,
        )
        return pat_auth(config)

    sql_conn = dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=credentials_provider,
    )
    try:
        yield sql_conn
    finally:
        sql_conn.close()
