#
# Copyright 2025 Tabs Data Inc.
#
from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_snowflake.bootest import TESTING_RESOURCES_PATH

import tabsdata as td
from tabsdata._secret import _recursively_evaluate_secret
from tabsdata._utils.logging import setup_tests_logging

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import logging

import pytest
from snowflake.connector import connect
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
)

logger = logging.getLogger(__name__)

REAL_CONNECTION_PARAMETERS = {
    "account": td.EnvironmentSecret("TD_SNOWFLAKE_ACCOUNT"),
    "user": td.EnvironmentSecret("TD_SNOWFLAKE_USER"),
    "password": td.EnvironmentSecret("TD_SNOWFLAKE_PAT"),
    "role": td.EnvironmentSecret("TD_SNOWFLAKE_ROLE"),
    "database": td.EnvironmentSecret("TD_SNOWFLAKE_DATABASE"),
    "schema": td.EnvironmentSecret("TD_SNOWFLAKE_SCHEMA"),
    "warehouse": td.EnvironmentSecret("TD_SNOWFLAKE_WAREHOUSE"),
}


def pytest_configure():
    setup_tests_logging()


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()


@pytest.fixture(scope="session")
def snowflake_connection():
    conn = connect(**_recursively_evaluate_secret(REAL_CONNECTION_PARAMETERS))
    try:
        yield conn
    finally:
        conn.close()
